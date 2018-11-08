import arcpy
from arcgis._impl.common._mixins import PropertyMap
import logging
import os
import copy
import xml.dom.minidom as DOM
import pandas as pd


class ServiceProcessException(Exception):
    """ Class to catch errors during service administration """

    def __init__(self, message):
        super().__init__(message)


class ServiceTransporter:
    """ This class is the base one to create a service. Has the most important dat to be able to create a service """

    def __init__(self, qualified_name, properties):
        """
        :param qualified_name: name of the service including the folder and service type
        e.g. folder_name\service_name.service_type
        :param properties: dictionary from service.properties
        """
        self.__qualified_name = qualified_name
        self.folder = None if not os.path.split(self.qualified_name)[0] else os.path.split(self.qualified_name)[0]
        self.properties = properties
        self.transferred = True
        self.transferred_comment = None
        self.copy_data_to_server = True
        self.overwrite_existing_service = False
        self.credits = 'No credits'
        self.sd_file = None
        self.sddraft_file = None
        self.name = self.properties['servicename']
        self.tags = self.properties['tags'] if 'tags' in self.properties else self.properties['servicename']
        self.description = 'No description defined yet' if not self.properties['description'] else\
            self.properties['description']
        self.type = self.properties['type']

    def __str__(self):
        return self.qualified_name

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return self.__class__ == other.__class and self.qualified_name == other.qualified_name

    @property
    def qualified_name(self):
        return self.__qualified_name

    @qualified_name.setter
    def qualified_name(self, x):
        pass

    def service_overview(self):
        """ Basic information with the status of the service we want to administrate
        :return: dictionary with basic information
        """
        return {'qualified_name': self.qualified_name, 'type': self.type, 'transferred': self.transferred,
                'transferred_comment': self.transferred_comment}


class STMapService(ServiceTransporter):
    """ Specialized ServiceTransporter class to create MapService services """

    def __init__(self, qualified_name, properties):
        """
        :param qualified_name: name of the service including the folder and service type
        e.g. folder_name\service_name.service_type
        :param properties: dictionary from service.properties
        """
        super(STMapService, self).__init__(qualified_name, properties)
        self.map_doc_path = None
        self.source_data = None
        self.target_data = None

    @property
    def enabled_extensions(self):
        """ List with dictionaries for the extesions in the service
        :return: list with enabled extensions
        """
        return self.properties['extensions']


class STGeocodeService(ServiceTransporter):

    def __init__(self, qualified_name, properties):
        """
        :param qualified_name: name of the service including the folder and service type
        e.g. folder_name\service_name.service_type
        :param properties: dictionary from service.properties
        """
        super(STGeocodeService, self).__init__(qualified_name, properties)
        self.loc_file_path = None
        self.server_connection_file = None
        self.from_root = None
        self.to_root = None


def create_service_transporter(server, *service_types):
    """ Create list with ServiceTransporter instances from list of services gotten form server. The function creates a
    ServiceTransporte instance for all the service in the server but flag as tranrvice and GeocodeService do not submit
    other service types.
    The type of property supported is PropertyMap. This must be aligned with the service type
    :param server: server from we want to get the list of services
    :param service_types: List of service types we want to get from server
    :return: list of ServiceTransporter
    """
    ser = []
    folders = server.services.folders
    folders = [x if x != '/' else None for x in folders]
    for folder in folders:
        services = server.services.list(folder, True)
        for service in services:
            if service.type in service_types:
                qualified_name = '{}\{}.{}'.format(folder, service.serviceName, service.type) if folder else\
                    '{}.{}'.format(service.serviceName, service.type)
                new_properties = dict()
                regular_dict(copy.deepcopy(service.properties), new_properties)
                if service.type == 'MapServer':
                    ser.append(STMapService(qualified_name, new_properties))
                elif service.type == 'GeocodeServer':
                    ser.append(STGeocodeService(qualified_name, new_properties))
                else:
                    s = ServiceTransporter(qualified_name, None)
                    s.transferred = False
                    s.transferred_comment = 'ServiceTransporter creation: The service type is not accepted'
                    ser.append(ServiceTransporter(qualified_name, None))
    return ser


def difference_in_service_list(source_services, target_services):
    """ Return the services in source server but not in target server
    :param source_services: list of ServerTransporter for source server
    :param target_services: list of ServerTransporter for target server
    :return: list of ServerTransporter
    """
    diff = set(source_services) - set(target_services)
    return diff


def service_document_path(source_services, root, *extensions):
    """ Look for the source document (mapx, mxd, loc ...) associated to the service. Source document must be in a
    similar directory structure than in the server. The folder structure is used to find the map document of each
    service. In case no map document is found the ServiceTransporter.transferred instance attribute is changed to False
    and a comment is added to ServciceTransporter.transferred_comment
    :param source_services: list of ServiceTransporter
    :param root: root of folder containing the map documents
    :param extensions: Service extension accepted
    :return: None
    """
    for service in source_services:
        files_path = os.path.join(root, service.qualified_name)
        files = process_directory(files_path, *extensions)
        try:
            if not files:
                raise ServiceProcessException('No source files to process for this service')
            if service.type == 'MapServer':
                if len(files) == 1:
                    service.map_doc_path = files[0]
                else:
                    raise ServiceProcessException('Problems looking for map documents')
            elif service.type == 'GeocodeServer':
                # All loc files within the service folder must be loaded
                service.loc_file_path = files
        except ServiceProcessException as e:
            service.map_doc_path = None
            service.transferred = False
            service.transferred_comment = str(e)


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
            logging.error('Map {} has not been imported'.format(map_doc))
    return maps


def regular_dict(from_elem, to_elem):
    """ Helper function to convert to lowercase the keys of  the PropertyMap or dictionary  passed as properties.
    There are some mismatches between the sddraft an the properties.
    :param from_elem: properties as dictionary
    :param to_elem: new dictionary created form the properties
    :return:
    """
    if isinstance(from_elem, (dict, PropertyMap)):
        for key, value in from_elem.items():
            if isinstance(value, (dict, PropertyMap)):
                to_elem[key.lower()] = {}
                regular_dict(value, to_elem[key.lower()])
            elif isinstance(value, list):
                to_elem[key.lower()] = []
                regular_dict(value, to_elem[key.lower()])
            else:
                to_elem[key.lower()] = value
    elif isinstance(from_elem, list):
        for value in from_elem:
            if isinstance(value, (dict, PropertyMap)):
                a = {}
                to_elem.append(a)
                regular_dict(value, a)
            elif isinstance(value, list):
                a = []
                to_elem.append(a)
                regular_dict(value, a)
            else:
                to_elem.append(value)


def set_location_file(loc_file, replace_from, replace_to):
    """ In case we want to publish a Composite the path of the loc files must be edited in the Composite file. This
    function replace the root
    :param loc_file: path of the loc file we need to edit
    :param replace_from: the original root
    :param replace_to: the new root
    :return:
    """
    with open(loc_file, 'r') as f:
        fileloc = f.read()
        fileloc1 = fileloc.replace(replace_from, replace_to)

    with open(loc_file, 'w') as f:
        f.write(fileloc1)


def publish_geocode_service(service: STGeocodeService):
    """  This function stgae the service and in case the process has not errors then upload the service definition to
    the server. If some exception is raised a new exception is created
    :param service: STGeocodeServie instance
    :return:
    :raise: ServiceProcessException in case things go wrong
    """
    try:
        arcpy.server.StageService(service.sddraft_file, service.sd_file)
    except arcpy.ExecuteError as e:
        logging.debug("Stage Service has raised an exception")
        logging.debug(arcpy.GetMessages(2))
        raise ServiceProcessException('Stage service exception: {}'.format(arcpy.GetMessages(2)))
    else:
        try:
            arcpy.server.UploadServiceDefinition(service.sd_file, service.server_connection_file)
        except arcpy.ExecuteError:
            logging.debug("An error occurred")
            logging.debug(arcpy.GetMessages(2))
            raise ServiceProcessException('Upload service error: {}'.format(arcpy.GetMessages(2)))


def set_sddraft_geocode(service: STGeocodeService, dummy_name):
    """ Create the sddraft for geocode service. If some error is reported the an exception is raised. An issue in
    arcpy.CreateGeocodeSDDraft has been found when SUGGEST is included in supported operations. So far only GEOCODE and
    REVERSE_GEOCODE are submitted
    :param service: instance of STGeocodeService
    :param dummy_name: prefix for the original service name
    :return:
    :raise: ServiceProcessException exception in case some an error is reported in the sddraf creation
    """

    capa = {'Geocode': 'GEOCODE', 'ReverseGeocode': 'REVERSE_GEOCODE', 'Suggest': 'SUGGEST'}
    # WARNING: The sddraf is not created if we pass the SUGGEST value, ValueError exception is raised
    capa = {'Geocode': 'GEOCODE', 'ReverseGeocode': 'REVERSE_GEOCODE'}

    if len(service.loc_file_path) > 1:
        # Change the path of the locators in the composite
        set_location_file(service.loc_file_path[0], service.from_root, service.to_root)

    capabilities = [capa[x] for x in service.properties['capabilities'].split(',') if x in capa]
    result = arcpy.CreateGeocodeSDDraft(loc_path=service.loc_file_path[0],
                                        out_sddraft=service.sddraft_file,
                                        service_name=dummy_name + service.name,
                                        server_type='FROM_CONNECTION_FILE',
                                        connection_file_path=service.server_connection_file,
                                        copy_data_to_server=service.copy_data_to_server,
                                        folder_name=service.folder,
                                        summary=service.description,
                                        tags=service.tags,
                                        max_result_size=int(service.properties['properties']['maxresultsize']),
                                        max_batch_size=int(service.properties['properties']['maxbatchsize']),
                                        suggested_batch_size=int(service.properties['properties']['suggestedbatchsize']),
                                        supported_operations=capabilities)

    if result['errors']:
        # If the sddraft analysis contained errors, display them
        logging.debug("Error were returned when creating service definition draft")
        logging.debug(result['errors'], indent=2)
        raise ServiceProcessException(result['errors'])


def custom_sddraft_mapservice(sddraft_doc, properties, *args):
    """ Modify the sddraft, according to the parameters passed.
    :param sddraft_doc: path to sddraft file
    :param properties: list of dictionaries with extensions as got from service.properties
    :param fs_capabilities: capabilities for feature server
    :return: Path to the new file. In case not new sddraft path to the previous one is returned
    """
    accepted_extensions = {'FeatureServer', 'WMSServer'}
    accepted_extensions.update(args)
    excluded_conf_properties = ['cacheDir', 'virtualOutputDir', 'outputDir', 'FilePath',
                                'virtualCacheDir', 'portalURL']

    excluded_ext_properties = ['onlineResource']

    doc = DOM.parse(sddraft_doc)
    unmatch = []
    for service_ext in properties['extensions']:
        if service_ext['enabled'] == 'true' and service_ext['typename'] in accepted_extensions:
            # <editor-fold desc="EXTENSION">
            type_names = doc.getElementsByTagName('TypeName')
            for type_name in type_names:
                if type_name.firstChild.data == service_ext['typename']:
                    extension = type_name.parentNode
                    for ext_element in extension.childNodes:
                        # <editor-fold desc="Enabled - Set the value of Enabled to true or to false ">
                        if ext_element.tagName == 'Enabled':
                            ext_element.firstChild.data = service_ext['enabled']  # 'true'
                        # </editor-fold>
                        # <editor-fold desc="Info - Section to set the capabilities and WebEnabled ">
                        if ext_element.tagName == 'Info':
                            for property_key in ext_element.getElementsByTagName('Key'):
                                if property_key.firstChild.data == 'WebCapabilities':
                                    property_key.nextSibling.firstChild.data = service_ext['capabilities']
                                elif property_key.firstChild.data == 'WebEnabled':
                                    property_key.nextSibling.firstChild.data = 'true'
                        # </editor-fold>
                        # <editor-fold desc="Properties - section for properties">
                        if ext_element.tagName == 'Props':
                            for property_key in ext_element.getElementsByTagName('Key'):
                                try:
                                    if property_key.firstChild.data in excluded_ext_properties:
                                        continue
                                    if property_key.nextSibling.hasChildNodes():
                                        property_key.nextSibling.firstChild.data = service_ext['properties'][
                                            property_key.firstChild.data.lower()]
                                except KeyError as k:
                                    unmatch.append('Key {} not in properties'.format(property_key.firstChild.data))
                                    logging.warning('Key {} not in properties'.format(property_key.firstChild.data))
                        # </editor-fold>
            # </editor-fold>
    # <editor-fold des="Change of Configuration Properties">
    conf_properties = doc.getElementsByTagName('ConfigurationProperties')
    prop_properties = properties['properties']
    for property_key in conf_properties[0].getElementsByTagName('Key'):
        try:
            if property_key.firstChild.data in excluded_conf_properties:
                continue
            if property_key.nextSibling.hasChildNodes():
                property_key.nextSibling.firstChild.data = prop_properties[property_key.firstChild.data.lower()]
        except KeyError as k:
            unmatch.append('Key {} not in properties'.format(property_key.firstChild.data))
            logging.warning('Key {} not in properties'.format(property_key.firstChild.data))
    # </editor-fold>
    root = os.path.split(sddraft_doc)[0]
    file_name = os.path.splitext(os.path.split(sddraft_doc)[1])[0]
    output_file = os.path.join(root, '{}{}.sddraft'.format(file_name, '_d'))
    f = open(output_file, 'w')
    doc.writexml(f)
    f.close()
    return output_file, unmatch


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
                logging.warning('Layer {} does not support CONNECTION PROPERTIES'.format(layer.name))
                raise ServiceProcessException('Layer does not support connection properties')

            if layer.connectionProperties and layer.connectionProperties['workspace_factory'] == 'SDE':
                if not source_data:
                    source_data = copy.deepcopy(layer.connectionProperties)
                    # validate argument is False
                    result = layer.updateConnectionProperties(source_data, target_data, True, True)
                    if not result:
                        logging.error('Layer {} connection not changed '.format(layer.name))
                        raise ServiceProcessException('connection change failed')
            else:
                cp = layer.connectionProperties['workspace_factory'] if layer.connectionProperties else\
                    'no connection properties'
                logging.error('Layer {} no enterprise database'.format(layer.name))
                raise ServiceProcessException('ERROR connection: {}'.format(cp))
        except ServiceProcessException as e:
            layers.append((layer.longName, str(e)))
        except arcpy.ExecuteWarning as e:
            layers.append((layer.longName, arcpy.GetMessages()))
        except arcpy.ExecuteError as e:
            layers.append((layer.longName, arcpy.GetMessages()))
    return layers


def acceptable_layer_type(arcgis_map, *acceptable_types):
    """ Say us if we have in a map a layer type we do no accept. The acceptable types are THREE_D, BASEMAP, FEATURE,
    GROUP, NETWORK_ANALYST, NETWORK_DATASET, RASTER, WEB .
    :param arcgis_map: arcgis Map instance
    :param acceptable_types:  list of the layers type we accept
    :return: True if we have only acceptable layers, False if we have some layer not acceptable
    """

    is_type = {'THREE_D': lambda x: x.is3DLayer, 'BASEMAP': lambda x: x.isBasemapLayer,
                     'FEATURE': lambda x: x.isFeatureLayer, 'GROUP': lambda x: x.isGroupLayer,
                     'NETWORK_ANALYST': lambda x: x.isNetworkAnalystLayer,
                     'NETWORK_DATASET': lambda x: x.isNetworkDatasetLayer, 'RASTER': lambda x: x.isRasterLayer,
                     'WEB': lambda x: x.isWebLayer}

    is_acceptable = True

    for l in arcgis_map.listLayers('*'):
        layer_type = ''
        for key, value in is_type.items():
            if value(l):
                layer_type = key
                break
        if layer_type not in acceptable_types:
            is_acceptable = False
            break

    return is_acceptable


def processing_geocode_service(service: STGeocodeService, dummy_name=''):
    """ Process to publish geocode services. In case some exception is raised ServiceTransporter.transferred is changed
    False and a comment is added to ServiceTransporter.transferred_comment
    :param service: STGeocode instance
    :param dummy_name: Prefix added to the original service name
    :return:
    """

    try:
        set_sddraft_geocode(service, dummy_name)
    except ServiceProcessException as e:
        logging.debug('Geocode Service sddraft error {}'.format(str(e)))
        service.transferred = False
        service.transferred_comment = str(e)
    else:
        try:
            publish_geocode_service(service)
        except ServiceProcessException as e:
            logging.debug('Geocode Service sddraft error {}'.format(str(e)))
            service.transferred = False
            service.transferred_comment = str(e)


def processing_mapservice(arcgis_proj, target_server, source_service, **kwargs):
    """ Create services in the target server based on the attributes of the ServiceTransporter.
    If an error is raised during the process the ServiceTransporter.transferred attribute is changed to False and a
    comment is added.
    :param arcgis_proj: arpy.mp.ArcGISProject instance
    :param target_server: instance of the server where we want to publish the service
    :param list_services: list of ServiceTransporter
    :param kwargs: service configuration can be changed but so far only the default configuration is supported
    :return: None
    """
    service_conf = {'server_type': 'FEDERATED_SERVER', 'service_type': 'MAP_IMAGE', 'dummy_name': ''}
    service_conf.update(kwargs)

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
                    raise ServiceProcessException('ERROR data connection change: {}'.format(msg.strip()))
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

                sharing_draft.offline = True

                sddraft_file = source_service.sddraft_file
                sd_file = source_service.sd_file
                sharing_draft.exportToSDDraft(sddraft_file)
                sddraft_file, unmatch = custom_sddraft_mapservice(sddraft_file, source_service.properties)

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



