import arcpy
import os
import copy
import xml.dom.minidom as DOM
import pandas as pd


class ServiceProcessException(Exception):
    """ Class to catch errors during service administration """

    def __init__(self, message):
        super().__init__(message)


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
        """ List with dictionaries for the extesions in the service
        :return: list with enabled extensions
        """
        return self.properties['extensions']


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


def set_sddraft_extensions(sddraft_doc, list_extensions, **kwargs):
    """ Modify the sddraft, according to the parameters passed. The method only apply default capabilities and only set
    enabled to true
    :param sddraft_doc: path to sddraft file
    :param list_extensions: list of dictionaries with extensions as got from service.properties
    :return: Path to the new file. In case not new sddraft path to the previous one is returned
    """
    default_cap = {'FeatureServer': 'Query,Create,Update,Delete,Uploads,Editing',
                   'WMSServer': 'GetCapabilities,GetMap,GetFeatureInfo,GetStyles,GetLegendGraphic,GetSchemaExtension'}
    default_cap.update(kwargs)
    doc = DOM.parse(sddraft_doc)
    changed = False

    for service_ext in list_extensions:
        if service_ext['enabled'] == 'true' and service_ext['typeName'] in default_cap.keys():
            changed = True
            type_names = doc.getElementsByTagName('TypeName')
            for type_name in type_names:
                if type_name.firstChild.data == service_ext['typeName']:
                    extension = type_name.parentNode
                    for ext_element in extension.childNodes:
                        if ext_element.tagName == 'Enabled':
                            ext_element.firstChild.data = 'true'
                        if ext_element.tagName == 'Info':
                            for props_array in ext_element.childNodes:
                                for prop_set in props_array.childNodes:
                                    for prop in prop_set.childNodes:
                                        if prop.tagName == 'Key':
                                            if prop.firstChild.data == 'WebCapabilities':
                                                if prop.nextSibling.hasChildNodes():
                                                    prop.nextSibling.firstChild.data = default_cap[service_ext['typeName']]
                                            if prop.firstChild.data == 'WebEnabled':
                                                if prop.nextSibling.hasChildNodes():
                                                    prop.nextSibling.firstChild.data = 'true'
    if changed:
        root = os.path.split(sddraft_doc)[0]
        file_name = os.path.splitext(os.path.split(sddraft_doc)[1])[0]
        output_file = os.path.join(root, '{}{}.sddraft'.format(file_name, '_d'))
        f = open(output_file, 'w')
        doc.writexml(f)
        f.close()
        return output_file
    else:
        return sddraft_doc


def change_connection(arcgis_map, target_data, source_data=None):
    """ Change the data source connection of each layer in a map. This function only works for layers with enterprise
     database connections. In the other cases the connection will not be changed and the name of the layer will be added
     to the returned list. In case not source_data is passed the existing one in the layer will be used.
    :param arcgis_map: the map to be processed
    :param source_data: original data source connection
    :param target_data: target data source connection
    :return: list of layers we could not change
    """
    layers = []
    for layer in arcgis_map.listLayers():
        try:
            if not layer.supports('CONNECTIONPROPERTIES'):
                raise ServiceProcessException('Layer does not support connection properties')
            if layer.connectionProperties and layer.connectionProperties['workspace_factory'] == 'SDE':
                if layer.isBroken:
                    raise ServiceProcessException('current connection not working')
                if not source_data:
                    source_data = copy.deepcopy(layer.connectionProperties)
                    # validate argument is False
                    result = layer.updateConnectionProperties(source_data, target_data, True, True)
                    if not result:
                        raise ServiceProcessException('connection change failed')
            else:
                cp = layer.connectionProperties['workspace_factory'] if layer.connectionProperties else\
                    'no connection properties'
                raise ServiceProcessException('ERROR connection: {}'.format(cp))
        except ServiceProcessException as e:
            layers.append((layer.longName, str(e)))
        except arcpy.ExecuteWarning as e:
            layers.append((layer.longName, arcpy.GetMessages()))
        except arcpy.ExecuteError as e:
            layers.append((layer.longName, arcpy.GetMessages()))
    return layers


def acceptable_layer_type(arcgis_map, *acceptable_types):
    is_acceptable = {'THREE_D': lambda x: x.is3DLayer, 'BASEMAP': lambda x: x.isBasemapLayer,
                     'FEATURE': lambda x: x.isFeatureLayer, 'GROUP': lambda x: x.isGroupLayer,
                     'NETWORK': lambda x: x.isNetworkAnalystLayer, 'RASTER': lambda x: x.isRasterLayer,
                     'WEB': lambda x: x.isWebLayer}

    for l in arcgis_map.listLayers('*'):
        l.is3DLayer
        l.isBasemapLayer
        l.isFeatureLayer
        l.isGroupLayer
        l.isNetworkAnalystLayer
        l.isRasterLayer
        l.isWebLayer


def create_service(arcgis_proj, target_server, list_services, **kwargs):
    """ Create services in the target server based on the attributes of the ServiceTransporter. In the case
    FeatureServer is enabled capabilities are 'Query,Create,Update,Delete,Editing,Uploads'. If an error is raised during
    the process the ServiceTransferred.transferred attribute is changed to False and a comment is added
    :param arcgis_proj: arpy.mp.ArcGISProject instance
    :param target_server: instance of the server where we want to publish the service
    :param list_services: list of ServiceTransporter
    :return: None
    """
    service_conf = {'server_type': 'FEDERATED_SERVER', 'service_type': 'MAP_IMAGE', 'dummy_name': ''}
    service_conf.update(kwargs)

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
            source_service.transferred_comment = 'Warning importing document msg: {}'.format(arcpy.GetMessages())
        except arcpy.ExecuteError as e:
            source_service.transferred = False
            source_service.transferred_comment = 'Error importing document msg: {}'.format(arcpy.GetMessages())
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
                        msg = ''
                        for r in result:
                            msg = msg + ' - ' + r[1]
                        raise ServiceProcessException('ERROR data connection {}'.format(msg.strip()))
                    else:
                        arcgis_proj.save()
            except ServiceProcessException as e:
                source_service.transferred = False
                source_service.transferred_comment = str(e)
            # </editor-fold>
            else:
                # <editor-fold desc="Stage service">
                try:
                    sharing_draft = my_map.getWebLayerSharingDraft(service_conf['server_type'],
                                                                   service_conf['service_type'],
                                                                   service_conf['dummy_name'] + source_service.name)
                    sharing_draft.credits = source_service.credits
                    sharing_draft.description = source_service.description
                    sharing_draft.copyDataToServer = source_service.copy_data_to_server
                    if source_service.folder:
                        sharing_draft.portalFolder = source_service.folder
                    sharing_draft.overwriteExistingService = source_service.overwrite_existing_service
                    sharing_draft.tags = source_service.tags

                    # In case we generate the draft offline. If False federatedSServerUrl must be provided
                    sharing_draft.offline = True

                    sddraft_file = source_service.sddraft_file
                    sd_file = source_service.sd_file
                    sharing_draft.exportToSDDraft(sddraft_file)
                    sddraft_file = set_sddraft_extensions(sddraft_file, source_service.enabled_extensions)

                except arcpy.ExecuteWarning as e:
                    source_service.transferred = False
                    source_service.transferred_comment = 'Warning in sddraft creation msg:' \
                                                         ' {}'.format(arcpy.GetMessages())
                except arcpy.ExecuteError as e:
                    source_service.transferred = False
                    source_service.transferred_comment = 'Error in sddraft creation msg:' \
                                                         ' {}'.format(arcpy.GetMessages())
                # </editor-fold>
                else:
                    try:
                        arcpy.StageService_server(sddraft_file, sd_file)
                    except arcpy.ExecuteWarning as e:
                        source_service.transferred = False
                        source_service.transferred_comment = 'Warning in stage service msg:' \
                                                             ' {}'.format(arcpy.GetMessages())
                    except arcpy.ExecuteError as e:
                        source_service.transferred = False
                        source_service.transferred_comment = 'Error in stage service msg:' \
                                                             ' {}'.format(arcpy.GetMessages())
                    else:
                        # <editor-fold desc="Uploading to server sections">
                        try:
                            result = target_server.services.publish_sd(sd_file, folder=source_service.folder)

                            if not result:
                                source_service.transferred = False
                                source_service.transferred_comment = 'Error in publish service definition msg: ' \
                                                                     'Server did not create the service'
                        except Exception as e:
                            source_service.transferred = False
                            source_service.transferred_comment = 'Error in publish service definition msg: {}'.format(
                                str(e))
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



