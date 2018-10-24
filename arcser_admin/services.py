import arcpy
import os
import copy
import xml.etree.ElementTree as et
import pandas as pd


class ServiceProcessException(Exception):
    """ Class to catch errors during service administration """

    def __init__(self, message, errors):
        super().__init__(message)
        self.errors = errors


class ServiceTransporter:
    """ This class has the necessary data to create a service. Is the one used to pass the service information from
    the original service or new one service to the server """

    def __init__(self, qualified_name, properties):
        """
        :param qualified_name: name of the service including the folder and service type
        e.g. folder_name\service_name.service_type
        :param properties: dictionary from service.properties
        """
        self.qualified_name = qualified_name
        self.folder = None if not os.path.split(self.qualified_name)[0] else os.path.split(self.qualified_name)[0]
        self.properties = properties
        self.map_doc_path = None
        self.transferred = True
        self.transferred_comment = None
        self.copy_data_to_server = True
        self.overwrite_existing_service = False
        self.credits = 'No credits'
        self.source_data = None
        self.target_data = None
        self.sd_file = None
        self.sddraft_file = None
        self.name = self.properties['serviceName']
        self.tags = self.properties['tags'] if 'tags' in self.properties else self.properties['serviceName']
        self.description = 'No description defined yet' if not self.properties['description'] else self.properties['description']
        self.type = self.properties['type']

    def service_overview(self):
        """ Basic information with the status of the service we want to administrate
        :return: dictionary with basic information
        """
        return {'qualified_name': self.qualified_name, 'type': self.type, 'transferred': self.transferred,
                'transferred_comment': self.transferred_comment}


    @property
    def enabled_extensions(self):
        """ List with enabled extensions in the service
        :return: list with enabled extensions
        """
        return [x['typeName'] for x in self.properties['extensions'] if x['enabled'] == 'true']


def create_service_transporter(server, *service_types):
    """ Create dictionary with ServiceTransporter instances from list of services gotten form server
    :param server: server from we want to get the list of services
    :param service_types: List of service types we want to get from server
    """
    ser = {}
    folders = server.services.folders
    folders = [x if x != '/' else None for x in folders]
    for folder in folders:
        services = server.services.list(folder, True)
        for service in services:
            if service.type in service_types:
                qualified_name = '{}\{}.{}'.format(folder, service.serviceName, service.type) if folder else\
                    '{}.{}'.format(service.serviceName, service.type)
                st = ServiceTransporter(qualified_name, copy.deepcopy(service.properties))
                ser[qualified_name] = st
    return ser


def difference_in_service_list(source_services, target_services):
    """ Return the services in source server but not in target server
    :param source_services: dictionary of ServerTransporter for source server
    :param target_services: dictionary of ServerTransporter for target server
    :return: list of ServerTransporter
    """

    d = set(source_services.keys()) - set(target_services.keys())
    return [source_services[x] for x in source_services.keys() if x in d]


def map_document_path(source_services, root, *extensions):
    """ Look for the map document (mapx, mxd ...) associated to the services. Map document must be in a similar directory
    structure than in the server. The folder structure is used to find the map document of each service. In case no map
    document is found the ServiceTransporter.transferred instance attribute is changed to False and a comment is added
    to ServciceTransporter.transferred_comment
    :param source_services: list of ServiceTransporter
    :param root: root of folder containing the map documents
    :param extensions: Service extension accepted
    :return: None
    """

    for service in source_services:
        files_path = os.path.join(root, service.qualified_name)
        files = process_directory(files_path, *extensions)
        if len(files) == 1:
            service.map_doc_path = files[0]
        else:
            service.map_doc_path = None
            service.transferred = False
            service.transferred_comment = 'Problems looking for map documents'


def process_directory(directory, *file_ext):
    """ Get all path of all files with a specific extension
    :param directory: directory where to look for the files
    :param file_ext: files extensions
    :return: list with paths
    """
    f = []
    for root, directory, files in os.walk(directory):
        for file in files:
            file_name, file_extension = os.path.splitext(file)
            if file_extension in file_ext:
                f.append(os.path.join(root, file))
    return f


def set_sddraft(sddraft_doc, extensions, fs_capabilities='Query,Create,Update,Delete,Uploads,Editing'):
    """ Modify the sddraft, according to the parameters passed.
    :param sddraft_doc: path to sddraft file
    :param extensions: extension we want to enable
    :param fs_capabilities: capabilities for feature server
    :return:
    """
    tree = et.parse(sddraft_doc)
    root = tree.getroot()

    for extension in root.iter('SVCExtension'):
        if extension.find('TypeName').text in extensions:
            extension.find('Enabled').text = 'true'
            if extension.find('TypeName').text == 'FeatureServer':
                for pro in extension.iter('PropertySetProperty'):
                    if pro.find('Key').text == 'WebCapabilities':
                        pro.find('Value').text = fs_capabilities

    root = os.path.split(sddraft_doc)[0]
    file_name = os.path.splitext(os.path.split(sddraft_doc)[1])[0]
    new_file = os.path.join(root, '{}{}.sddraft'.format(file_name, '_d'))
    tree.write(new_file)
    return new_file


def import_map_document(arc_proj, *map_docs):
    """ Import maps document in a project. It is recommended to import only one document at a time
    :param arc_proj: arcpy.mp.ArcGISProject instance
    :param map_docs: paths to map dos to be imported
    :return: List of map document do not imported
    """
    maps = []
    for map_doc in map_docs:
        try:
            arc_proj.importMapDocument(map_doc)
        except (arcpy.ExecuteWarning, arcpy.ExecuteError) as e:
            maps.append(map_doc)
        except Exception as e:
            maps.append(map_doc)
    return maps


def change_connection(arcgis_map, source_data, target_data):
    """ Change the data source connection of each layer in a map. This function only works for layers with database
    connections
    :param arcgis_map: the map to be processed
    :param source_data: original data source connection
    :param target_data: target data source connection
    :return: list of layers we could not change
    """
    # WARNING: Review -> http://pro.arcgis.com/en/pro-app/arcpy/mapping/updatingandfixingdatasources.htm
    layers = []
    for layer in arcgis_map.listLayers():
        if layer.connectionProperties:
            try:
                # WARNING: Value for validate should be set to True and we should manage the case where the change is not done
                layer.updateConnectionProperties(source_data, target_data, True, False)
            except arcpy.ExecuteWarning as e:
                print('Geoprocessing Tool error {}'.format(arcpy.GetMessage(1)))
                layers.append(layer.longName)
            except arcpy.ExecuteError as e:
                print('Geoprocessing Tool error {}'.format(arcpy.GetMessage(2)))
                layers.append(layer.longName)
    return layers


def create_service(arcgis_proj, target_server, list_services):
    """ Create services in the target server based on the attributes of the ServiceTransporter. In the case
    FeatureServer is enabled capabilities are 'Query,Create,Update,Delete,Editing,Uploads'. If an error is raised during
    the process the ServiceTransferred.transferred attribute is changed to False and a comment is added
    :param arcgis_proj: arpy.mp.ArcGISProject instance
    :param target_server: instance of the server where we want to publish the service
    :param list_services: list of ServiceTransporter
    :return: None
    """

    server_type = {'MapServer': 'FEDERATED_SERVER', 'OTHER': 'HOSTING_SERVER'}
    service_type = {'MapServer': 'MAP_IMAGE', 'OTHER_1': 'FEATURE', 'OTHER_2': 'TILE'}

    counter = 0
    for source_service in list_services:
        counter += 1
        print('Processing {}/{}'.format(counter, len(list_services)))
        maps_in_project = [x.name for x in arcgis_proj.listMaps('*')]
        # <editor-fold desc="Import document">
        try:
            arcgis_proj.importDocument(source_service.map_doc_path)
            arcgis_proj.save()
        except arcpy.ExecuteWarning as e:
            source_service.transferred = False
            source_service.transferred_comment = 'Warning importing document msg: {}'.format(arcpy.GetMessage(1))
        except arcpy.ExecuteError as e:
            source_service.transferred = False
            source_service.transferred_comment = 'Error importing document msg: {}'.format(arcpy.GetMessage(2))
        # </editor-fold>
        else:
            # <editor-fold desc="Change data source">
            my_map = None
            for m in arcgis_proj.listMaps('*'):
                if m.name not in maps_in_project:
                    my_map = arcgis_proj.listMaps(m.name)[0]
                    break
            try:
                if source_service.target_data:
                    result = change_connection(my_map, source_service.source_data, source_service.target_data)
                    if result:
                        raise ServiceProcessException('Raised exception during database connection change')
                    else:
                        arcgis_proj.save()
            except ServiceProcessException as e:
                source_service.transferred = False
                source_service.transferred_comment = str(e)
            # </editor-fold>
            else:
                # <editor-fold desc="Stage service">
                try:
                    sharing_draft = my_map.getWebLayerSharingDraft('FEDERATED_SERVER', 'MAP_IMAGE', source_service.name)
                    sharing_draft.credits = source_service.credits
                    sharing_draft.description = source_service.description
                    sharing_draft.copyDataToServer = source_service.copy_data_to_server
                    if source_service.folder:
                        sharing_draft.portalFolder = source_service.folder
                    sharing_draft.overwriteExistingService = source_service.overwrite_existing_service
                    sharing_draft.tags = source_service.tags

                    # In case we generate the draft offline
                    sharing_draft.offline = True

                    sddraft_file = source_service.sddraft_file
                    sd_file = source_service.sd_file
                    sharing_draft.exportToSDDraft(sddraft_file)

                except arcpy.ExecuteWarning as e:
                    source_service.transferred = False
                    source_service.transferred_comment = 'Warning in sddraft creation msg:' \
                                                         ' {}'.format(arcpy.GetMessage(1))
                except arcpy.ExecuteError as e:
                    source_service.transferred = False
                    source_service.transferred_comment = 'Error in sddraft creation msg:' \
                                                         ' {}'.format(arcpy.GetMessage(2))
                # </editor-fold>
                else:
                    try:
                        arcpy.StageService_server(sddraft_file, sd_file)
                    except arcpy.ExecuteWarning as e:
                        source_service.transferred = False
                        source_service.transferred_comment = 'Warning in stage service msg:' \
                                                             ' {}'.format(arcpy.GetMessage(1))
                    except arcpy.ExecuteError as e:
                        source_service.transferred = False
                        source_service.transferred_comment = 'Error in stage service msg:' \
                                                             ' {}'.format(arcpy.GetMessage(2))
                    else:
                        # <editor-fold desc="Uploading to server sections">
                        try:
                            pass
                            result = target_server.services.publish_sd(sd_file, folder=source_service.folder)
                            if not result:
                                source_service.transferred = False
                                source_service.transferred_comment = 'Error in publish service definition msg: ' \
                                                                     'Server did not create the service'
                        except arcpy.ExecuteWarning as e:
                            source_service.transferred = False
                            source_service.transferred_comment = 'Warning in publish service definition msg:' \
                                                                 ' {}'.format(arcpy.GetMessage(1))
                        except arcpy.ExecuteError as e:
                            source_service.transferred = False
                            source_service.transferred_comment = 'Error in publish service definition msg:' \
                                                                 ' {}'.format(arcpy.GetMessage(2))
                        except Exception as e:
                            source_service.transferred = False
                            source_service.transferred_comment = 'Error in publish service definition'

                        # </editor-fold>


def report_to_csv(service_transporters, csv_file):
    """ Basic report using the basic description of each service
    :param service_transporters: list of ServiceTransporter
    :param csv_file: path to create the csv file
    :return: None
    """
    v = []
    for t in service_transporters:
        v.append(t.service_overview())

    df = pd.DataFrame(v)
    df.to_csv(csv_file, encoding='utf-8', sep='|')



