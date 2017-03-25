# etcdmate
Utility that helps an Etcd Member join a cluster


The instance that runs this tool needs to have an AWS IAM instance role with the following policies:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "autoscaling:DescribeAutoScalingGroups",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": "autoscaling:DescribeAutoScalingInstances",
            "Resource": "*"
        }
    ]
}
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "ec2:Describe*",
            "Resource": "*"
        }
    ]
}
```
