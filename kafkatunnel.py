#!/usr/bin/env python3
import subprocess
import sys

import click

from Instance import AWSInstances, ManualInstances


@click.group(
    help="Access kafka/zookeeper via ssh tunnel to consume and produce messages from your local machine"
)
def cli():
    pass


@cli.command(
    help="retrieve kafka/zookeeper ip's from AWS (important: a resource tag with Name=kafka/zookeeper is needed)"
)
@click.argument("jump_host")
@click.option("-zp", "--zookeeper_port", default="2181")
@click.option("-kp", "--kafka_port", default="9092")
@click.option("-r", "--region", default="ap-southeast-2")
@click.option("-p", "--profile", default="default")
def aws(jump_host, zookeeper_port, kafka_port, region, profile):
    instances = []
    click.echo(
        " * retrieving ip's from AWS ({},{}) zookeeper/kafka ec2 instances by tag_name ...".format(
            profile, region
        )
    )
    aws = AWSInstances(profile, region)
    instances += aws.getIps("zookeeper", zookeeper_port)
    instances += aws.getIps("kafka", kafka_port)
    connect(jump_host, instances)


@cli.command(help="provide the IP's of your zookeeper/kafka")
@click.argument("jump_host")
@click.argument("zookeeper_ips")
@click.argument("kafka_ips")
@click.argument("schemaregistry_ips", default="")
@click.option("-zp", "--zookeeper_port", default="2181")
@click.option("-kp", "--kafka_port", default="9092")
@click.option("-sp", "--schemaregistry_port", default="8081")
def manual(
    jump_host,
    zookeeper_ips,
    kafka_ips,
    schemaregistry_ips,
    zookeeper_port,
    kafka_port,
    schemaregistry_port,
):
    instances = []
    click.echo(" * using manual ip's ...")
    man = ManualInstances()
    instances += man.getIps("zookeeper", zookeeper_ips, zookeeper_port)
    instances += man.getIps("kafka", kafka_ips, kafka_port)
    if schemaregistry_ips:
        instances += man.getIps("schemareg", schemaregistry_ips, schemaregistry_port)
    connect(jump_host, instances)


def connect(jump_host, instances):
    try:
        print_instances(instances)
        add_local_interfaces(instances)
        update_hosts_file(instances, "add")
        connect_ssh_tunnel(jump_host, instances)
    finally:
        click.echo(" * Cleaning up: Removing local interfaces and updating hosts file.")
        remove_local_interfaces(instances)
        update_hosts_file(instances, "remove")


def add_local_interfaces(instances):
    click.echo(" * adding interface, user password might be needed")
    for instance in instances:
        if sys.platform == "darwin":
            cmd = ["sudo", "ifconfig", "lo0", "alias", instance.ip]
        else:
            cmd = ["sudo", "ip", "add", "a", "dev", "lo", instance.ip]
        subprocess.call(cmd)


def remove_local_interfaces(instances):
    click.echo(" * removing interface, user/root password might be needed")
    update_hosts_file(instances, "remove")
    for instance in instances:
        if sys.platform == "darwin":
            cmd = ["sudo", "ifconfig", "lo0", "delete", instance.ip]
        else:
            cmd = ["sudo", "ip", "del", "a", "dev", "lo", instance.ip]
        subprocess.call(cmd)


def print_instances(instances):
    click.echo("")
    for i in instances:
        click.echo("{:<10} on {:<15} port {:>5}".format(i.name, i.ip, i.port))
    click.echo("")


def connect_ssh_tunnel(jump_host: str, instances: list) -> None:
    click.echo(" * connecting to jump host " + jump_host)
    opts = []
    for i in instances:
        opts += [
            "-v",
            "-N",
            "-L",
            "{ip}:{port}:{ip}:{port}".format(ip=i.ip, port=i.port),
        ]
    subprocess.call(["ssh"] + opts + [jump_host])


def update_hosts_file(instances, action="add"):
    hosts_path = "/etc/hosts"
    marker_start = "# START KAFKATUNNEL\n"
    marker_end = "# END KAFKATUNNEL\n"
    with open(hosts_path, "r+") as hosts_file:
        lines = hosts_file.readlines()
        hosts_file.seek(0)
        if action == "add":
            # Remove any existing KAFKATUNNEL entries
            in_block = False
            for line in lines:
                if line == marker_start:
                    in_block = True
                    continue
                if line == marker_end:
                    in_block = False
                    continue
                if not in_block:
                    hosts_file.write(line)
            # Add new KAFKATUNNEL entries
            hosts_file.write(marker_start)
            for instance in instances:
                dns = ip_to_dns(instance.ip, "eu-west-1.compute.internal")
                hosts_file.write(f"{instance.ip} {dns}\n")
            hosts_file.write(marker_end)
        else:  # Remove entries
            in_block = False
            for line in lines:
                if line == marker_start:
                    in_block = True
                    continue
                if line == marker_end:
                    in_block = False
                    continue
                if not in_block:
                    hosts_file.write(line)
        hosts_file.truncate()


def ip_to_dns(ip: str, domain: str = "example.com") -> str:
    """
    Convert an IP address to a DNS name by replacing dots with dashes and appending a domain.

    Args:
    ip (str): The IP address to convert.
    domain (str): The domain name to append. Defaults to 'example.com'.

    Returns:
    str: A DNS name constructed from the IP address.
    """
    dns_name = "ip-" + ip.replace(".", "-") + "." + domain
    return dns_name


if __name__ == "__main__":
    cli()
