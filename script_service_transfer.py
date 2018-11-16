import arcgis
import arcpy
import os
import logging
from arcser_admin.services import create_service_transporter, difference_in_service_list, service_document_path,\
    processing_mapservice, report_to_csv, processing_geocode_service


def main(portal_source, user_source, password_source, portal_target, user_target, password_target, services_folder_path,
         source_connection, target_connection, worksapce, arcgis_project, prefix_service_name, default_folder,
         report_output, root_from, root_to, server_connection_file):
    """
    :param portal_source: portal source to copy the data
    :param user_source: user of portal source
    :param password_source: password of portal source
    :param portal_target: portal target
    :param user_target: user portal target
    :param password_target: password portal target
    :param services_folder_path: folder where the mxd and mapx are located
    :param source_connection: dictionary of the source connection when working with  sde
    :param target_connection: dictionary of the target connection  when working with sde
    :param worksapce: folder where we the script will create the structure of directories for sd and sddraft files
    :param arcgis_project: path to the arcgis project
    :param prefix_service_name: prefix added to the service in case wew want to identify them
    :param default_folder: folder where to create the service in the server if we want put all of them in the same folder
    :param report_output: csv for report
    :param root_from: original root of loc files
    :param root_to: target root loc files
    :param server_connection_file: path to server connection file
    :return:
    """

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
    target_service = create_service_transporter(target_server, 'MapServer', 'GeocodeServer')
    # </editor-fold>

    # <editor-fold desc="Only the ones no in the target service are selected">
    transfer_services = difference_in_service_list(source_service, target_service)
    # </editor-fold>

    logging.debug('Services for transfer {}'.format(len(transfer_services)))
    service_document_path([x for x in transfer_services if x.transferred], services_folder_path, *['.mapx', '.mxd', '.loc'])

    subset_transfer_services = [x for x in transfer_services if x.transferred]

    for serv in subset_transfer_services:
        if serv.type == 'GeocodeServer':
            serv.server_connection_file = server_connection_file
            serv.from_root = root_from
            serv.to_root = root_to
        if serv.type == 'MapServer':
            serv.source_data = source_connection
            serv.target_data = target_connection

    root = worksapce

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
        print('Service type {}'.format(s.type))
        print('Service processed {}/{}'.format(counter, len(subset_transfer_services)))

        if s.type == 'MapServer':
            processing_mapservice(arcgis_proj, dummy_name=prefix_service_name)
        elif s.type == 'GeocodeServer':
            processing_geocode_service(s,  dummy_name=prefix_service_name)
    report_to_csv(source_service, report_output)


if __name__ == '__main__':
    main(portal_source='',
         user_source='',
         password_source='',
         portal_target='',
         user_target='',
         password_target='',
         services_folder_path='',
         source_connection='',
         target_connection='',
         worksapce='',
         arcgis_project='',
         prefix_service_name='',
         default_folder='',
         report_output='',
         root_from='',
         root_to='',
         server_connection_file='')

