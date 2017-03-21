package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/ec2"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/viruxel/etcdmate/etcdclient"
)

var (
	version    = "0.1.1"
	dropInFile = kingpin.Flag(
		"drop-in-file",
		"The systemd drop-in file to create.",
	).Default(
		"/var/run/systemd/system/etcd2.service.d/50-etcdmate.conf",
	).Envar(
		"ETCDMATE_DROP_IN_FILE",
	).String()
	timeout = kingpin.Flag(
		"timeout",
		"Timeout waiting for etcd requests to respond.",
	).Default(
		"5s",
	).Envar(
		"ETCDMATE_TIMEOUT",
	).Duration()
	clientSchema = kingpin.Flag(
		"client-schema",
		"The Etcd client schema.",
	).Default(
		"http",
	).Envar(
		"ETCDMATE_CLIENT_SCHEMA",
	).HintOptions(
		"http",
		"https",
	).Enum("http", "https")
	clientPort = kingpin.Flag(
		"client-port",
		"The Etcd client port",
	).Default(
		"2379",
	).Envar(
		"ETCDMATE_CLIENT_PORT",
	).Int()
	peerSchema = kingpin.Flag(
		"peer-schema",
		"The Etcd peer schema.",
	).Default(
		"http",
	).Envar(
		"ETCDMATE_PEER_SCHEMA",
	).HintOptions(
		"http",
		"https",
	).Enum("http", "https")
	peerPort = kingpin.Flag(
		"peer-port",
		"The Etcd peer port",
	).Default(
		"2380",
	).Envar(
		"ETCDMATE_PEER_PORT",
	).Int()
	caFile = kingpin.Flag(
		"ca-file",
		"verify certificates of HTTPS-enabled servers using this CA bundle",
	).Envar(
		"ETCDMATE_CA_FILE",
	).Default("").String()
	certFile = kingpin.Flag(
		"cert-file",
		"identify HTTPS client using this SSL certificate file",
	).Envar(
		"ETCDMATE_CERT_FILE",
	).Default("").String()
	keyFile = kingpin.Flag(
		"key-file",
		"identify HTTPS client using this SSL key file",
	).Envar(
		"ETCDMATE_KEY_FILE",
	).Default("").String()
)

func main() {
	kingpin.Version(version)
	kingpin.Parse()
	log.Printf("Drop-in file: %s\n", *dropInFile)
	log.Printf("Timeout: %s\n", *timeout)
	log.Printf("Client schema: %s\n", *clientSchema)
	log.Printf("Client port: %d\n", *clientPort)
	log.Printf("Peer schema: %s\n", *peerSchema)
	log.Printf("Peer port: %d\n", *peerPort)

	localSess := session.Must(session.NewSession())
	metadata, err := GetMetadata(localSess)
	if err != nil {
		log.Fatal(err)
	}
	sess := localSess.Copy(&aws.Config{
		Region: aws.String(metadata.Region),
	})
	expectedMembers, err := GetExpectedMembers(sess, metadata.InstanceID)
	if err != nil {
		log.Fatal(err)
	}
	etcdClient, err := etcdclient.NewClient(
		*caFile,
		*certFile,
		*keyFile,
		*timeout,
	)
	if err != nil {
		log.Fatal(err)
	}
	healthyMember, err := etcdClient.FindHealthyMember(expectedMembers)
	if err != nil {
		// The cluster is not up. Assume new cluster
		log.Println(err)
		WriteDropIn(expectedMembers, "new")
		os.Exit(0)
	}
	existingMembers, err := etcdClient.ListMembers(healthyMember)
	if err != nil {
		log.Println(err)
		WriteDropIn(expectedMembers, "new")
		os.Exit(0)
	}
	RemoveStaleMembers(
		etcdClient,
		healthyMember,
		expectedMembers,
		existingMembers,
	)
	myself := GetMyself(expectedMembers, metadata.InstanceID)
	MaybeAddMyself(
		etcdClient,
		healthyMember,
		existingMembers,
		myself,
	)
	WriteDropIn(expectedMembers, "existing")
}

func GetMetadata(sess *session.Session) (ec2metadata.EC2InstanceIdentityDocument, error) {
	metadata := ec2metadata.New(sess)
	if !metadata.Available() {
		return ec2metadata.EC2InstanceIdentityDocument{}, errors.New("Not An AWS EC2 instance")
	}
	id, err := metadata.GetInstanceIdentityDocument()
	if err != nil {
		return ec2metadata.EC2InstanceIdentityDocument{}, err
	}
	log.Printf("Metadata: %+v\n", id)
	return id, nil
}

func GetAsg(svc *autoscaling.AutoScaling, insId string) (string, error) {
	log.Println("Looking for Autoscaling group of instance", insId)
	params := &autoscaling.DescribeAutoScalingInstancesInput{
		InstanceIds: []*string{&insId},
		MaxRecords:  aws.Int64(1),
	}
	resp, err := svc.DescribeAutoScalingInstances(params)
	if err != nil {
		return "", err
	}
	asgName := resp.AutoScalingInstances[0].AutoScalingGroupName
	log.Println("Found Autoscaling group", *asgName)
	return *asgName, nil
}

func GetAsgInstanceIds(svc *autoscaling.AutoScaling, asgName string) ([]*string, error) {
	log.Println("Looking for instances in Autoscaling group", asgName)
	params := &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []*string{&asgName},
		MaxRecords:            aws.Int64(1),
	}
	resp, err := svc.DescribeAutoScalingGroups(params)
	if err != nil {
		return []*string{}, err
	}
	instances := resp.AutoScalingGroups[0].Instances
	instanceIds := []*string{}
	for _, instance := range instances {
		log.Printf("Found instance %+v\n", instance)
		if *instance.LifecycleState == "InService" {
			instanceIds = append(instanceIds, instance.InstanceId)
		} else {
			log.Println("Ignoring instance", *instance.InstanceId)
		}
	}
	return instanceIds, nil
}

func GetEC2Instances(sess *session.Session, instanceIds []*string) ([]ec2.Instance, error) {
	svc := ec2.New(sess)
	params := &ec2.DescribeInstancesInput{
		InstanceIds: instanceIds,
	}
	resp, err := svc.DescribeInstances(params)
	if err != nil {
		return []ec2.Instance{}, err
	}
	instances := []ec2.Instance{}
	for _, reservation := range resp.Reservations {
		for _, instance := range reservation.Instances {
			instances = append(instances, *instance)
		}
	}
	return instances, nil
}

func GetExpectedMembers(sess *session.Session, insId string) ([]etcdclient.Member, error) {
	etcdMembers := []etcdclient.Member{}
	asg := autoscaling.New(sess)
	asgName, err := GetAsg(asg, insId)
	if err != nil {
		return etcdMembers, err
	}
	instanceIds, err := GetAsgInstanceIds(asg, asgName)
	if err != nil {
		return etcdMembers, err
	}
	instances, err := GetEC2Instances(sess, instanceIds)
	if err != nil {
		return etcdMembers, err
	}
	for _, instance := range instances {
		etcdMembers = append(etcdMembers, etcdclient.Member{
			Name: *instance.InstanceId,
			ClientURL: fmt.Sprint(
				*clientSchema,
				"://",
				*instance.PrivateIpAddress,
				":",
				*clientPort,
			),
			PeerURL: fmt.Sprint(
				*peerSchema,
				"://",
				*instance.PrivateIpAddress,
				":",
				*peerPort,
			),
		})
	}
	log.Printf("Expected Members %+v\n", etcdMembers)
	return etcdMembers, nil
}

func RemoveStaleMembers(
	c etcdclient.Client,
	hm etcdclient.Member,
	expectedMembers []etcdclient.Member,
	existingMembers []etcdclient.Member,
) {
	Expected := func(exiM etcdclient.Member) bool {
		for _, expM := range expectedMembers {
			if exiM.Name == expM.Name {
				return true
			}
		}
		return false
	}
	for _, exiM := range existingMembers {
		if !Expected(exiM) {
			err := c.RemoveMember(hm, exiM)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func GetMyself(expectedMembers []etcdclient.Member, insId string) etcdclient.Member {
	for _, member := range expectedMembers {
		if member.Name == insId {
			return member
		}
	}
	panic(errors.New(fmt.Sprint(
		"Couldn't find instance in expected members", insId,
	)))
}

func MaybeAddMyself(
	c etcdclient.Client,
	hm etcdclient.Member,
	existingMembers []etcdclient.Member,
	myself etcdclient.Member,
) {
	exists := false
	for _, member := range existingMembers {
		if member.Name == myself.Name {
			exists = true
		}
	}
	if !exists {
		err := c.AddMember(hm, myself)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func WriteDropIn(expectedMembers []etcdclient.Member, state string) {
	initCluster := []string{}
	for _, member := range expectedMembers {
		initCluster = append(initCluster, fmt.Sprint(
			member.Name,
			"=",
			member.PeerURL,
		))
	}
	err := os.MkdirAll(path.Dir(*dropInFile), 0777)
	if err != nil {
		log.Fatal(err)
	}
	file, err := os.Create(*dropInFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	fmt.Fprintf(
		file,
		`[Service]
Environment=ETCD_INITIAL_CLUSTER=%s
Environment=ETCD_INITIAL_CLUSTER_STATE=%s
`,
		strings.Join(initCluster, ","),
		state,
	)
}
