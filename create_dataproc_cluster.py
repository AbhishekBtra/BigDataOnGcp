import sys
from yaml import safe_load


# [START dataproc_create_cluster]
from google.cloud import dataproc_v1 as dataproc


def create_cluster(dataproc_config):
    """Creating a Cloud Dataproc cluster using the Python client library.
    Args:
        dataproc_config YAML
    """
    # [START dataproc_create_cluster]

    #env(dev/prd) specific
    env= dataproc_config['env']
    zone= dataproc_config['zone']
    region= dataproc_config['region']
    workload= dataproc_config['workload']
    dataproc_sa = dataproc_config['dataproc_sa']
    dtp_project_id= dataproc_config['dtp_project_id']

    #onprem cluster specific
    on_prem_name_node_1= dataproc_config['on_prem_name_node_1']
    on_prem_name_node_2= dataproc_config['on_prem_name_node_2']
    on_prem_hdfs_nameservice= dataproc_config['on_prem_hdfs_nameservice']

    #keys and pwds
    key= dataproc_config['key']
    keyring_name= dataproc_config['keyring_name']
    security_realm= dataproc_config['security_realm']
    keyring_location= dataproc_config['keyring_location']
    root_principal_password_uri = dataproc_config['root_principal_password_uri']
    cross_realm_trust_shared_password_uri=dataproc_config['cross_realm_trust_shared_password_uri']

    #dp cluster specific
    data_proc_tags= dataproc_config['data_proc_tags']
    idle_delete_ttl= dataproc_config['idle_delete_ttl']
    worker_num_instances= dataproc_config['worker_num_instances']
    gcs_bucket_for_dataproc= dataproc_config['gcs_bucket_for_dataproc']
    master_config_machine_type=dataproc_config['master_config_machine_type']
    worker_config_machine_type=dataproc_config['worker_config_machine_type']

    #realm specific
    cross_realm_trust_kdc= dataproc_config['cross_realm_trust_kdc']
    cross_realm_trust_realm= dataproc_config['cross_realm_trust_realm']
    cross_realm_trust_admin_server= dataproc_config['cross_realm_trust_admin_server']

    #network specific
    subnetwork_uri_project= dataproc_config['subnetwork_uri_project']
    subnetwork_uri_subnetworks= dataproc_config['subnetwork_uri_subnetworks']

    #derive cluster id
    from random import randint
    random_number = str(randint(100, 999))
    dataproc_cluster_id = f'cluster-{workload}-data-offload-{env}-f{random_number}'

    # Create a client with the endpoint set to the desired cluster region.
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    
    CLUSTER_CONFIG = {
        "config_bucket": gcs_bucket_for_dataproc,
        "temp_bucket": gcs_bucket_for_dataproc,
        "gce_cluster_config": {
            "service_account" : dataproc_sa,
            "tags"            : data_proc_tags,
            "zone_uri"        : zone,
            "internal_ip_only": True,
            "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
            "subnetwork_uri": f"projects/{subnetwork_uri_project}/regions/{region}/subnetworks/{subnetwork_uri_subnetworks}",
            "metadata" : { "startup-script-url": f"gs://{gcs_bucket_for_dataproc}/scripts/cluster-init-scripts/startup.sh", \
            "shutdown-script-url":f"gs://{gcs_bucket_for_dataproc}/scripts/cluster-init-scripts/shutdown.sh"
            }
        },
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": master_config_machine_type,
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
        },
        "worker_config": {
            "num_instances": worker_num_instances,
            "machine_type_uri": worker_config_machine_type,
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
        },
        "software_config": {
            "image_version": "2.0.45-debian10",
            "properties": {
                "dataproc:dataproc.allow.zero.workers":"true",
                "core:hadoop.rpc.protection":"authentication",
                "hdfs:dfs.encrypt.data.transfer":"false",
                "hdfs:dfs.data.transfer.protection":"authentication",
                f"hdfs:dfs.ha.namenodes.{on_prem_hdfs_nameservice}":"nn1,nn2",
                f"hdfs:dfs.client.failover.proxy.provider.{on_prem_hdfs_nameservice}":"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
                f"hdfs:dfs.ha.automatic-failover.enabled.{on_prem_hdfs_nameservice}":"true",
                f"hdfs:dfs.namenode.rpc-address.{on_prem_hdfs_nameservice}.nn1":f"{on_prem_name_node_1}:8020",
                f"hdfs:dfs.namenode.http-address.{on_prem_hdfs_nameservice}.nn1":f"{on_prem_name_node_1}:20101",
                f"hdfs:dfs.namenode.rpc-address.{on_prem_hdfs_nameservice}.nn2":f"{on_prem_name_node_2}:8020",
                f"hdfs:dfs.namenode.http-address.{on_prem_hdfs_nameservice}.nn2":f"{on_prem_name_node_2}:20101",
                "hdfs:dfs.nameservices":f"{dataproc_cluster_id},{on_prem_hdfs_nameservice}",
                "yarn:yarn.scheduler.minimum-allocation-mb":b'256',
                "core:hadoop.security.auth_to_local":"RULE:[1:$1@$0](dataproc-.+@.*)s/.*/dataproc//\n    RULE:[1:$1](.*)s/(.*)/$1/g\n    RULE:[2:$1](.*)s/(.*)/$1/g\n    DEFAULT"
            }
        },

        "security_config": {
            "kerberos_config": {
            "enable_kerberos"                       : True,
            "kms_key_uri"                           : f"projects/{dtp_project_id}/locations/{keyring_location}/keyRings/{keyring_name}/cryptoKeys/{key}",
            "root_principal_password_uri"           : root_principal_password_uri,
            "realm"                                 : security_realm,
            "cross_realm_trust_admin_server"        : cross_realm_trust_admin_server,
            "cross_realm_trust_kdc"                 : cross_realm_trust_kdc,
            "cross_realm_trust_realm"               : cross_realm_trust_realm,
            "cross_realm_trust_shared_password_uri" : cross_realm_trust_shared_password_uri
            }
        },
        "endpoint_config":{
            "enable_http_port_access": True
        }
    }

    
    # Create the cluster config.
    cluster = {
        "project_id": dtp_project_id,
        "cluster_name": dataproc_cluster_id,
        "config": CLUSTER_CONFIG,
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": dtp_project_id, "region": region, "cluster": cluster}
    )
    result = operation.result()

    # Output a success message.
    print(f"Cluster created successfully: {result.cluster_name}")
    # [END dataproc_create_cluster]


if __name__ == "__main__":

    import os

    if len(sys.argv) < 2:
        sys.exit("Missing argument - dir path for dataproc yaml config")
    
    config_dp_file_path = sys.argv[1]

    #yamls in gcs dir
    dp_config_files = os.listdir(config_dp_file_path) 

    #read YAML
    for config_file in dp_config_files:

        if config_file.endswith('.yaml'):

            with open(f'{config_dp_file_path}/{config_file}','r') as yaml_config:

                data = yaml_config.read()
                dp_config = safe_load(data)
                
                #create cluster with configs
                create_cluster(dp_config)

