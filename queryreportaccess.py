#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script to query CNS volumes in a vSAN environment.

This script requires PyVmomi and VSAN API libraries.
"""

__author__ = "Jim McCann"

import sys
import ssl
import atexit
import argparse
import getpass
import vsanapiutils

from pyVmomi import vim
from pyVim.connect import SmartConnect, Disconnect

import pyVmomi
import vsanmgmtObjects

def main():
    args = GetArgs()

    # Connect to vCenter and get CNS volume manager stub
    (vc_service_inst, cns_volume_manager) = connect_to_servers(args)

    # Choose between querying or deleting volumes
    action = input("Choose action (query/delete): ")


    if action.lower() == "query":
        # Prepare query filter
        filter_spec = vim.cns.QueryFilter()

        # Optionally, specify volume names to filter (uncomment and add names)
        # filter_spec.names = ["volume_name1", "volume_name2"]

        # Query CNS volumes
        volume_query_result = cns_volume_manager.Query(filter_spec)
        print(volume_query_result.volumes)
        # Open a file for writing output
        with open("cns_volumes_access.txt", "w") as output_filea:

            # Check if any volumes were found
            if volume_query_result is None:
                print("No CNS volumes found.", file=output_filea)
            else:
                print("Found CNS volumes:", file=output_filea)
                for volume in volume_query_result.volumes:
                    if volume.datastoreAccessibilityStatus == "accessible":
                       print(f"- Volume Name: {volume.name}", file=output_filea)
                       print(f"  - HEALTH: {volume.datastoreAccessibilityStatus}", file=output_filea)
                       print(f"  - CLUSTER_ID: {volume.metadata.containerCluster.clusterId}", file=output_filea)
                       print(f"  - USER: {volume.metadata.containerCluster.vSphereUser}", file=output_filea)
                       print(f"  - VOLUME_ID: {volume.volumeId.id}", file=output_filea)
                       print("-" * 52, file=output_filea)  # Optional separator for each volume
    
        print("Output saved to cns_volumes_access.txt")

        with open("cns_volumes_nonaccess.txt", "w") as output_filena:
            # Check if any volumes were found
            if volume_query_result is None:
                print("No CNS volumes found.", file=output_filena)
            else:
                print("Found CNS volumes:", file=output_filena)
                for volume in volume_query_result.volumes:
                    if volume.datastoreAccessibilityStatus == "notAccessible":
                       print(f"- Volume Name: {volume.name}", file=output_filena)
                       print(f"  - HEALTH: {volume.datastoreAccessibilityStatus}", file=output_filena)
                       print(f"  - CLUSTER_ID: {volume.metadata.containerCluster.clusterId}", file=output_filena)
                       print(f"  - USER: {volume.metadata.containerCluster.vSphereUser}", file=output_filena)
                       print(f"  - VOLUME_ID: {volume.volumeId.id}", file=output_filena)
                       print("-" * 52, file=output_filena)  # Optional separator for each volume

        print("Output saved to cns_volumes_nonaccess.txt")

    elif action.lower() == "delete":
        # Input the volume ID to delete
        volume_id = input("Enter the volume ID to delete: ")

        try:
            volume_id_obj = vim.cns.VolumeId(id=volume_id)  # Create a VolumeId object
            cns_delete_task = cns_volume_manager.Delete([volume_id_obj], deleteDisk=True)
            vcTask = vsanapiutils.ConvertVsanTaskToVcTask(cns_delete_task, vc_service_inst._stub)
            vsanapiutils.WaitForTasks([vcTask], vc_service_inst)
            print(('Delete CNS volume task finished with status: %s' %
                          vcTask.info.state))
            if vcTask.info.error is not None:
               msg = "Delete CNS volume failed with error '{0}'".format(vcTask.info.error)
               sys.exit(msg)
#            print(f"Delete CNS volume task finished with status: {vcTask.info.state}")
#            if vcTask.info.error is not None:
#                msg = f"Delete CNS volume failed with error: {vcTask.info.error}"
#                sys.exit(msg)
        except vim.fault.VimFaultException as e:
            print(f"Error deleting volume: {e}")

    else:
        print("Invalid action. Please choose 'query' or 'delete'.")


    # Disconnect from vCenter
    Disconnect(vc_service_inst)

def GetArgs():
    """
    Parses command-line arguments for vCenter connection.
    """
    parser = argparse.ArgumentParser(description="Query CNS volumes")
    parser.add_argument('-s', '--host', required=True,
                        help='Remote vCenter host to connect to')
    parser.add_argument('-o', '--port', type=int, default=443,
                        help='Port to connect on')
    parser.add_argument('-u', '--user', required=True,
                        help='User name to use when connecting to host')
    parser.add_argument('-p', '--password', required=False,
                        help='Password to use when connecting to host')
    return parser.parse_args()

def connect_to_servers(args):
    """
    Connects to vCenter and retrieves CNS volume manager stub.

    Similar to the original script, handles SSL context and user input.
    """
    if args.password:
        password = args.password
    else:
        password = getpass.getpass(prompt='Enter password for host %s and '
                                        'user %s: ' % (args.host, args.user))

    ssl_context = None
    if sys.version_info[:3] > (2, 7, 8):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    vc_service_inst = SmartConnect(host=args.host,
                                    user=args.user,
                                    pwd=password,
                                    port=int(args.port),
                                    sslContext=ssl_context)
    atexit.register(Disconnect, vc_service_inst)

    api_version = vsanapiutils.GetLatestVmodlVersion(args.host, int(args.port))
    vsan_stub = vsanapiutils.GetVsanVcMos(vc_service_inst._stub,
                                         context=ssl_context,
                                         version=api_version)

    cns_volume_manager = vsan_stub['cns-volume-manager']

    return vc_service_inst, cns_volume_manager

if __name__ == "__main__":
    main()
