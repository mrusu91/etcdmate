package etcdclient

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func NewClient(caFile, certFile, keyFile string, timeout time.Duration) (Client, error) {
	tlsConfig := &tls.Config{}
	// Load client cert
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return Client{}, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
		tlsConfig.BuildNameToCertificate()
	}
	// Load CA cert
	if caFile != "" {
		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			return Client{}, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}
	httpClient := &http.Client{Timeout: timeout}
	if caFile != "" || certFile != "" {
		transport := &http.Transport{TLSClientConfig: tlsConfig}
		httpClient.Transport = transport
	}
	return Client{httpClient: httpClient}, nil
}

type Client struct {
	httpClient *http.Client
}

func (c *Client) FindHealthyMember(members []Member) (Member, error) {
	for _, member := range members {
		url := fmt.Sprintf("%s/health", member.ClientURL)
		log.Println("Checking etcd member health at", url)
		resp, err := c.httpClient.Get(url)
		// if can't access the member, assume member not exists
		if err != nil {
			log.Println(err)
			continue
		}
		var jresp map[string]string
		json.NewDecoder(resp.Body).Decode(&jresp)
		resp.Body.Close()
		if jresp["health"] != "true" {
			log.Printf("Unhealthy member %#v\n", member)
			continue
		} else {
			log.Println("Healthy member %#v\n", member)
			return member, nil
		}
	}
	return Member{}, errors.New("No healthy member found")
}

func (c *Client) RemoveMember(hm Member, rm Member) error {
	log.Println("Removing member", rm)
	url := fmt.Sprintf("%s/v2/members/%s", hm.ClientURL, rm.ID)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	log.Println("Member removed")
	return nil
}

func (c *Client) AddMember(hm Member, am Member) error {
	log.Println("Adding member", am)
	url := fmt.Sprintf("%s/v2/members", hm.ClientURL)
	byteData := []byte(fmt.Sprintf(
		"{\"name\": \"%s\", \"peerURLs\": [\"%s\"]}",
		am.Name,
		am.PeerURL,
	))
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(byteData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	log.Println("Member added")
	return nil
}

func (c *Client) ListMembers(hm Member) ([]Member, error) {
	url := fmt.Sprintf("%s/v2/members", hm.ClientURL)
	log.Println("Listing members using url", url)
	members := []Member{}
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return members, err
	}
	defer resp.Body.Close()
	var jresp map[string][]jsonMember
	json.NewDecoder(resp.Body).Decode(&jresp)
	for _, jm := range jresp["members"] {
		m := Member{
			ID:   jm.Id,
			Name: jm.Name,
		}
		if len(jm.ClientURLs) > 0 {
			m.ClientURL = jm.ClientURLs[0]
		}
		if len(jm.PeerURLs) > 0 {
			m.PeerURL = jm.PeerURLs[0]
		}
		members = append(members, m)
	}
	log.Println("Found members", members)
	return members, nil
}

type Member struct {
	ID        string
	Name      string
	ClientURL string
	PeerURL   string
}

// Needed to marshal json response for listing members
type jsonMember struct {
	Id         string
	Name       string
	ClientURLs []string
	PeerURLs   []string
}
