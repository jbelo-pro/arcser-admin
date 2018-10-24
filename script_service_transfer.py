import arcgis
import arcpy
import os
from helpers.global_variables import PORTALS, USERS, PASSWORDS, BASIC_ROOT, FOLDER_MAP_DOCUMENTS, ARCGIS_PROJ
from arcser_admin.services import create_service_transporter, difference_in_service_list, map_document_path,\
    create_service, report_to_csv
from helpers.global_variables import CONNECTIONS, REPORT_CSV


source_gis = arcgis.gis.GIS(PORTALS['DEV'], USERS['DEV'], PASSWORDS['DEV'])
target_gis = arcgis.gis.GIS(PORTALS['QC'], USERS['QC'], PASSWORDS['QC'])

# get first the services
source_server = source_gis.admin.servers.list()[0]
target_server = target_gis.admin.servers.list()[0]

source_service = create_service_transporter(source_server, 'MapServer')
target_service = create_service_transporter(target_server, 'MapServer')

transfer_services = difference_in_service_list(source_service, target_service)

print('Services for transfer {}'.format(len(transfer_services)))

# TODO: It must be  removed . Only for testing
# map_document_path will set the value transferred to False if there is not a mad doc file
map_document_path(transfer_services,FOLDER_MAP_DOCUMENTS,
                  *['.mapx', '.mxd'])

subset_transfer_services = [x for x in transfer_services if x.transferred]


# subset_transfer_services = subset_transfer_services[:15]

for serv in subset_transfer_services:
    serv.source_data = CONNECTIONS['DEV']
    serv.target_data = CONNECTIONS['QC']

root = BASIC_ROOT

for serv in subset_transfer_services:
    serv.sddraft_file = os.path.join(root, serv.qualified_name, serv.name + '.sddraft')
    serv.sd_file = os.path.join(root, serv.qualified_name, serv.name + '.sd')
    os.makedirs(os.path.join(root, serv.qualified_name), exist_ok=True)

print()
for x in subset_transfer_services:
    x.folder = 'test'

print()

arcgis_proj = arcpy.mp.ArcGISProject(ARCGIS_PROJ)

create_service(arcgis_proj, target_server, subset_transfer_services)

report_to_csv(transfer_services, REPORT_CSV)

print()
