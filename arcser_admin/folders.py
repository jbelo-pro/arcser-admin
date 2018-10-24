from arcser_admin.helpers import ServerException


def replicate_folders_server(source_gis, target_gis):
    """ Create folders in target server from the list of folders in source server.
    :param source_gis: source GIS
    :param target_gis: target GIS
    :return: list of folders not created.
    :raise: ServerException in case servers can not be retrieved
    """
    folders = []
    try:
        servers_source = source_gis.admin.servers.list()
        servers_target = target_gis.admin.servers.list()

        if len(servers_source) != 1 or len(servers_target) != 1:
            raise ServerException('Environments do not have same amount of servers')

        folders_source = servers_source[0].services.folders
        folders_target = servers_target[0].services.folders

    except (ServerException, Exception) as e:
        raise ServerException(str(e))
    else:
        folders_source = [folder for folder in folders_source if folder not in folders_target]
        for folder in folders_source:
            try:
                if not servers_target[0].services.create_folder(folder):
                    folders.append(folder)
            except Exception as e:
                folders.append(folder)

    return folders

