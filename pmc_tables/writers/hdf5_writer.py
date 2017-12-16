import pmc_tables


def debug_df(df):
    columns = list(df.columns)
    for i in range(len(columns)):
        columns[i] = (columns[i], df.iloc[0, i])
    df = df.drop(df.index[0], axis=0)
    return df


def write_record_to_hdf5(info, data, store):
    pmc_id = info['pmc_id']
    for table_id, table_info in data.items():
        table_df = table_info.pop('table_df')
        table_df = pmc_tables.process_dataframe(table_df)
        table_df = table_df.convert_objects()
        table_key = f"/{pmc_id}{table_id}"
        _try_writing_table(table_key, table_df, store)
        pmc_tables.write_hdf5_metadata(table_key, table_info, store)
    pmc_tables.write_hdf5_metadata(f"/{pmc_id}", info, store)


def _try_writing_table(key, df, store):
    debug_functions = iter([debug_df])
    while True:
        try:
            pmc_tables.write_hdf5_table(key, df, store)
            return
        except Exception as e:
            try:
                fn = next(debug_functions)
            except StopIteration:
                raise e
            df = fn(df)
