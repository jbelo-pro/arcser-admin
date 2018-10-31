import arcgis
import arcpy
from arcser_admin.services import create_service_transporter, difference_in_service_list, map_document_path, \
    services_mapserver, report_to_csv
from arcser_admin.helpers import slicer
import tempfile
import shutil
from multiprocessing import Pool
import os
import logging


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
TEMPS_FOLDER = ''  # Path to temp folder where to
ARC_PROJ_TEMPLATE = '' # Template arcgis project


def create_service_dec(project, portal, user, password, slice_):
        arcgis_proj = arcpy.mp.ArcGISProject(project)
        my_gis = arcgis.GIS(portal, user, password)
        server = my_gis.admin.servers.list()[0]
        services_mapserver(arcgis_proj, server, slice_)


def main():
    source_gis = arcgis.gis.GIS(PORTAL_SOURCE, USER_SOURCE, PASSWORD_SOURCE)
    target_gis = arcgis.gis.GIS(PORTAL_TARGET, USER_TARGET, PASSWORD_TARGET)

    # get first the services
    source_server = source_gis.admin.servers.list()[0]
    target_server = target_gis.admin.servers.list()[0]

    source_service = create_service_transporter(source_server, 'MapServer')
    target_service = create_service_transporter(target_server, 'MapServer')

    transfer_services = difference_in_service_list(source_service, target_service)

    logging.debug('Services for transfer {}'.format(len(transfer_services)))

    map_document_path(transfer_services, MAP_FOLDER_PATH, *['.mapx', '.mxd'])

    subset_transfer_services = [x for x in transfer_services if x.transferred]

    with Pool(4) as p:

        tasks = []

        for slice_ in slicer(subset_transfer_services, 20):
            for serv in slice_:
                serv.source_data = SOURCE_CONNECTION
                serv.target_data = TARGET_CONNECTION

            root = WORKSAPCE

            for serv in subset_transfer_services:
                serv.sddraft_file = os.path.join(root, serv.qualified_name, serv.name + '.sddraft')
                serv.sd_file = os.path.join(root, serv.qualified_name, serv.name + '.sd')
                os.makedirs(os.path.join(root, serv.qualified_name), exist_ok=True)

            if DEFAULT_FOLDER:
                for x in subset_transfer_services:
                    x.folder = DEFAULT_FOLDER

            temp_folder = tempfile.mkdtemp(dir=TEMPS_FOLDER)
            shutil.copy2(ARC_PROJ_TEMPLATE, os.path.join(temp_folder, 'arcgis_proj.aprx'))

            pro = os.path.join(temp_folder, 'arcgis_proj.aprx')
            task = p.apply_async(create_service_dec, (pro, PORTAL_TARGET, USER_TARGET, PASSWORD_TARGET, slice_))
            tasks.append(task)

        for task in tasks:
            task.wait()
            logging.debug('Task is done {}'.format(task.get()))

    report_to_csv(transfer_services, REPORT_OUTPUT)


if __name__ == '__main__':
    main()
