import logging

from localbricks.delta_sharing_loader import DeltaSharingLoader
from localbricks.local_storage_loader import LocalStorageLoader



def data_loader_manager(action: str, profile_path: str = None, table_config_path: str = None, share: str = None, schema_table_name: str = None, table_path: str = None, schema_tables: list = None, update_all: bool = False, limit: int = 10) -> None:
    """
    Manages data loading actions for Delta Sharing and local storage.
    The following args are necessary for each action:
        -action load_directly: mandatory to provide share and schema_table_name
        -action load_from_local_storage: mandatory to provide schema_table_name
        -action update_add_tables_local_storage: mandatory to provide update all or schema_tables

    Args:
        action (str): The action to perform. Options are 'load_directly', 'load_from_local_storage', and 'update_add_tables_local_storage'.
        profile_path (Optional[str]): The path to the Delta Sharing profile. Defaults to None.
        table_config_path (Optional[str]): The path to the table configuration JSON file. Defaults to None.
        share (Optional[str]): The share name. Required for 'load_directly'.
        schema_table_name (Optional[str]): The schema table name (or alias if present). Required for 'load_directly' and 'load_from_local_storage'.
        table_path (Optional[str]): The path to the local storage table. Defaults to None.
        schema_tables (Optional[List[str]]): The list of schema tables to update. Required for 'update_add_tables_local_storage'.
        update_all (bool): Flag to indicate if all tables should be updated. Defaults to False.
        limit (int): The number of rows to limit the DataFrame. Defaults to 100.

    Returns:
        Optional[DataFrame]: The loaded DataFrame, or None if the action is 'update_add_tables_local_storage'.

    Raises:
        ValueError: If an invalid action is provided.
        Exception: If an error occurs during the action execution.
    """
    
    logging.info(f"Called data_loader_manager with action={action}, profile_path={profile_path}, table_config_path={table_config_path}, share={share}, schema_table_name={schema_table_name}, table_path={table_path}, schema_tables={schema_tables}, update_all={update_all}")

    try:        

        if action == 'load_directly':
            if not (share and schema_table_name):
                raise ValueError("For action 'load_directly', 'share' and 'schema_table_name' are required")
            df = DeltaSharingLoader(profile_path= profile_path, table_config_path= table_config_path).directly_load_dataframe(share= share, schema_table_name= schema_table_name, limit= limit)
        elif action == 'load_from_local_storage':
            if not schema_table_name:
                raise ValueError("For action 'load_from_local_storage', 'schema_table_name' is required")
            df = LocalStorageLoader(schema_table_name= schema_table_name, table_path= table_path).load_dataframe_from_local_storage()
        elif action == 'update_add_tables_local_storage':
            if not (update_all or schema_tables):
                raise ValueError("For action 'update_add_tables_local_storage', 'update_all' or 'schema_tables' must be provided")
            DeltaSharingLoader(profile_path= profile_path, table_config_path= table_config_path).control_update_storage(schema_tables= schema_tables, update_all= update_all)
            df = None
        else:
            raise ValueError(f"Action {action} not implemented")
        
        return df

    except Exception as e:

        logging.error(f"Error performing action {action} for table {schema_table_name}: {e}")
        raise

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    # Basic example usage
    delta_share_table_alias = "client-tui.flight_plan_data__joao_test2"
    df = data_loader_manager(action= 'load_directly', share = 'localbricks', schema_table_name = delta_share_table_alias)
    df.show()
