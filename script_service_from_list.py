import arcgis
import arcpy
import os
import logging
from arcser_admin.services import create_service_transporter, service_document_path,\
    processing_mapservice, report_to_csv, processing_geocode_service, ServiceTransporter
import pandas as pd
from pandas import DataFrame

from test_global_variables import CONNECTIONS


def main(portal_source, user_source, password_source, portal_target, user_target, password_target, services_folder_path,
         source_connection, target_connection, workspace, arcgis_project, prefix_service_name, default_folder,
         report_output, root_from, root_to, server_connection_file, list_service_to_copy, delete):
    """
    :param portal_source: Portal source to copy the data
    :param user_source: User of portal source
    :param password_source: Password of portal source
    :param portal_target: Portal target
    :param user_target: User portal target
    :param password_target: Password portal target
    :param services_folder_path: Folder where the mxd and mapx are located
    :param source_connection: Dictionary of the source connection when working with  SDE
    :param target_connection: Dictionary of the target connection  when working with SDE
    :param worksapce: Folder where we the script will create the structure of directories for sd and sddraft files
    :param arcgis_project: Path to the arcgis project
    :param prefix_service_name: Prefix added to the service in case wew want to identify them
    :param default_folder: Folder where to create the service in the server if we want put all of them in the same folder
    :param report_output: CSV for report
    :param root_from: Original root of loc files
    :param root_to: Target root loc files
    :param server_connection_file: Path to server connection file
    :param list_service_to_copy: Path to csv file to
    :return:
    """

    service_df: DataFrame = pd.read_csv(list_service_to_copy, sep='|', encoding='utf-8',
                                        names=['service_name'])
    service_for_copy = [x for x in service_df['service_name']]

    # reference to portal we wnat to use
    result = arcpy.SignInToPortal(portal_url='https://dfs-arcgis-71.dpkodev.un.org/arcgis', username='jbelo01',
                         password='Abcd1234')

    print(result)

    # <editor-fold desc="Creation of the arcgis.GIS instances">
    source_gis = arcgis.gis.GIS(portal_source, user_source, password_source)
    target_gis = arcgis.gis.GIS(portal_target, user_target, password_target)
    # </editor-fold>

    # <editor-fold desc="Getting the servers
    source_server = source_gis.admin.servers.list()[0]
    target_server = target_gis.admin.servers.list()[0]
    # </editor-folder>

    # <editor-fold desc="Get the list of ServiceTransporter. Only the Mapservers are loaded">
    source_service = create_service_transporter(source_server, 'MapServer', 'GeocodeServer')
    # </editor-fold>

    # <editor-fold desc="Check if the service in the source are there">
    for s in source_service:
        if s.qualified_name.replace('\\', '/') not in service_for_copy:
            s.transferred = False
    # </editor-fold>

    # <editor-fold desc="Delete the services we have in the target server">
    folders = target_server.services.folders
    if delete:
        for folder in folders:
            for s1 in target_server.services.list(folder, True):
                sn = '{}/{}.{}'.format(folder, s1.serviceName, s1.type) if folder != '/' else\
                    '{}.{}'.format(s1.serviceName, s1.type)
                if sn in service_for_copy:
                    print('{} is in '.format(sn))
                    # s1.delete()
    # </editor-fold>

    service_document_path([x for x in source_service if x.transferred], services_folder_path, *['.mapx', '.mxd', '.loc'])
    subset_transfer_services = [x for x in source_service if x.transferred]

    for serv in subset_transfer_services:
        if serv.type == 'GeocodeServer':
            serv.server_connection_file = server_connection_file
            serv.from_root = root_from
            serv.to_root = root_to
        if serv.type == 'MapServer':
            serv.source_data = source_connection
            serv.target_data = target_connection
            serv.federated_server = 'https://dfs-arcgis-71.dpkodev.un.org:6443/arcgis'

    root = workspace

    # <editor-fold desc="Creation of directories">
    for serv in subset_transfer_services:
        serv.sddraft_file = os.path.join(root, serv.qualified_name, serv.name + '.sddraft')
        serv.sd_file = os.path.join(root, serv.qualified_name, serv.name + '.sd')
        os.makedirs(os.path.join(root, serv.qualified_name), exist_ok=True)
    # </editor-fold>

    if default_folder:
        for x in subset_transfer_services:
            x.folder = default_folder

    arcgis_proj = arcpy.mp.ArcGISProject(arcgis_project)

    counter = 0
    for s in subset_transfer_services:
        counter += 1
        print('Service {} type {}'.format(s.qualified_name, s.type))
        print('Service processed {}/{}'.format(counter, len(subset_transfer_services)))
        print('')
        if s.type == 'MapServer':
            processing_mapservice(arcgis_proj, s, dummy_name=prefix_service_name)
        elif s.type == 'GeocodeServer':
            processing_geocode_service(s,  dummy_name=prefix_service_name)
    report_to_csv(source_service, report_output)


if __name__ == '__main__':
    main(portal_source='https://uat-geoportal.dfs.un.org/arcgis',
         user_source='geocortexadmin',
         password_source='tHas2eju',
         portal_target='https://dfs-arcgis-71.dpkodev.un.org/arcgis',
         user_target='jbelo01',
         password_target='Abcd1234',
         services_folder_path='D:\workspace\services_uat_to_dev\COPY_UAT',
         source_connection=None,
         target_connection=CONNECTIONS['DEV'],
         workspace=r'D:\workspace\services_uat_to_dev\FOLDER_SD',
         arcgis_project=r'D:\workspace\services_uat_to_dev\uat_to_dev\uat_to_dev.aprx',
         prefix_service_name='TEST_W_',
         default_folder='test',
         report_output='D:\\workspace\\services_uat_to_dev\\output_report.csv',
         root_from='\\\\dfs-isilon-01.dpko.un.org\\DFS-GIS-02_CIFS2\\uat_ags\\arcgisserver\\directories\\arcgissystem\\arcgisinput',
         root_to='D:\\workspace\\services_uat_to_dev\\COPY_UAT',
         server_connection_file='D:\\workspace\\server_connection_file\\DEV_SERVER.ags',
         list_service_to_copy='D:\\workspace\\services_uat_to_dev\\control_task_services.csv',
         delete=False)
