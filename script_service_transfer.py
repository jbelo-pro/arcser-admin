import arcgis
import arcpy
import os
import logging
from arcser_admin.services import create_service_transporter, difference_in_service_list, map_document_path,\
    services_mapserver, report_to_csv


PORTAL_SOURCE = ''  # Portal source to copy the data
USER_SOURCE = ''  # User of portal source
PASSWORD_SOURCE = ''  # Password of portal source
PORTAL_TARGET = ''  # Portal target
USER_TARGET = ''  # User portal target
PASSWORD_TARGET = ''  # Password portal target
MAP_FOLDER_PATH = ''  # Folder where the mxd and mapx are located
SOURCE_CONNECTION = ''  # Dictionary of the source connection when working with  SDE
TARGET_CONNECTION = ''  # Dictionary of the target connection  when working with SDE
WORKSAPCE = ''  # Folder where we the script will create the structure of directories for sd and sddraft files
ARCGIS_PROJECT = ''  # Path to the arcgis project
PREFIX_SERVICE_NAME = ''  # Prefix added to the service in case wew want to identify them
DEFAULT_FOLDER = ''  # Folder where to create the service in the server if we want put all of them in the same folder
REPORT_OUTPUT = ''  # CSV for report


def main():

    # <editor-fold desc="Creation of the arcgis.GIS instances">
    source_gis = arcgis.gis.GIS(PORTAL_SOURCE, USER_SOURCE, PASSWORD_SOURCE)
    target_gis = arcgis.gis.GIS(PORTAL_TARGET, USER_TARGET, PASSWORD_TARGET)
    # </editor-fold>

    # <editor-fold desc="Getting the servers
    source_server = source_gis.admin.servers.list()[0]
    target_server = target_gis.admin.servers.list()[0]
    # </editor-folder>

    # <editor-fold desc="Get the list of ServiceTransporter. Only the Mapservers are loaded">
    source_service = create_service_transporter(source_server, 'MapServer')
    target_service = create_service_transporter(target_server, 'MapServer')
    # </editor-fold>

    # <editor-fold desc="Only the ones no in the target service are selected">
    transfer_services = difference_in_service_list(source_service, target_service)
    # </editor-fold>

    logging.debug('Services for transfer {}'.format(len(transfer_services)))

    map_document_path(transfer_services, MAP_FOLDER_PATH, *['.mapx', '.mxd'])

    subset_transfer_services = [x for x in transfer_services if x.transferred]

    for serv in subset_transfer_services:
        serv.source_data = SOURCE_CONNECTION
        serv.target_data = TARGET_CONNECTION

    root = WORKSAPCE

    # <editor-fold desc="Creation of directories">
    for serv in subset_transfer_services:
        serv.sddraft_file = os.path.join(root, serv.qualified_name, serv.name + '.sddraft')
        serv.sd_file = os.path.join(root, serv.qualified_name, serv.name + '.sd')
        os.makedirs(os.path.join(root, serv.qualified_name), exist_ok=True)
    # </editor-fold>

    if DEFAULT_FOLDER:
        for x in subset_transfer_services:
            x.folder = DEFAULT_FOLDER

    arcgis_proj = arcpy.mp.ArcGISProject(ARCGIS_PROJECT)

    services_mapserver(arcgis_proj, target_server, subset_transfer_services, dummy_name=PREFIX_SERVICE_NAME)
    report_to_csv(transfer_services, REPORT_OUTPUT)


if __name__ == '__main__':
    main()
