import arcgis
import arcpy
from arcser_admin.services import create_service_transporter, difference_in_service_list, service_document_path, \
    processing_mapservice, report_to_csv
from arcser_admin.helpers import slicer
import tempfile
import shutil
from multiprocessing import Pool
import os
import logging


def create_service_dec(project, portal, user, password, slice_):
        arcgis_proj = arcpy.mp.ArcGISProject(project)
        my_gis = arcgis.GIS(portal, user, password)
        server = my_gis.admin.servers.list()[0]
        for s in slice_:
            processing_mapservice(arcgis_proj)


def main(portal_source, user_source, password_source, portal_target, user_target, password_target, services_folder_path,
         source_connection, target_connection, workspace, default_folder, report_output, temps_folder,
         arc_proj_template):
    """
    :param portal_source: Portal source to copy the data
    :param user_source: User of portal source
    :param password_source: Password of portal source
    :param portal_target: Portal target
    :param user_target:  User portal target
    :param password_target: Password portal target
    :param services_folder_path: Folder where the mxd and mapx are located
    :param source_connection: Dictionary of the source connection when working with  SDE
    :param target_connection: Dictionary of the target connection  when working with SDE
    :param workspace: Folder where we the script will create the structure of directories for sd and sddraft files
    :param default_folder: Folder where to create the service in the server if we want put all of them in the same folder
    :param report_output: CSV for report
    :param temps_folder: Path to temp folder where to
    :param arc_proj_template: Template arcgis project
    :return:
    """

    source_gis = arcgis.gis.GIS(portal_source, user_source, password_source)
    target_gis = arcgis.gis.GIS(portal_target, user_target, password_target)

    # get first the services
    source_server = source_gis.admin.servers.list()[0]
    target_server = target_gis.admin.servers.list()[0]

    source_service = create_service_transporter(source_server, 'MapServer')
    target_service = create_service_transporter(target_server, 'MapServer')

    transfer_services = difference_in_service_list(source_service, target_service)

    logging.debug('Services for transfer {}'.format(len(transfer_services)))

    service_document_path(transfer_services, services_folder_path, *['.mapx', '.mxd'])

    subset_transfer_services = [x for x in transfer_services if x.transferred]

    with Pool(4) as p:

        tasks = []

        for slice_ in slicer(subset_transfer_services, 20):
            for serv in slice_:
                serv.source_data = source_connection
                serv.target_data = target_connection

            root = workspace

            for serv in subset_transfer_services:
                serv.sddraft_file = os.path.join(root, serv.qualified_name, serv.name + '.sddraft')
                serv.sd_file = os.path.join(root, serv.qualified_name, serv.name + '.sd')
                os.makedirs(os.path.join(root, serv.qualified_name), exist_ok=True)

            if default_folder:
                for x in subset_transfer_services:
                    x.folder = default_folder

            temp_folder = tempfile.mkdtemp(dir=temps_folder)
            shutil.copy2(arc_proj_template, os.path.join(temp_folder, 'arcgis_proj.aprx'))

            pro = os.path.join(temp_folder, 'arcgis_proj.aprx')
            task = p.apply_async(create_service_dec, (pro, portal_target, user_target, password_target, slice_))
            tasks.append(task)

        for task in tasks:
            task.wait()
            logging.debug('Task is done {}'.format(task.get()))

    report_to_csv(transfer_services, report_output)


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
         workspace='',
         default_folder='',
         report_output='',
         temps_folder='',
         arc_proj_template='')
