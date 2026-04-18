def write_to_redshift(df, table, url, properties, mode="overwrite"):
    df.write \
        .mode(mode) \
        .jdbc(
            url=url,
            table=table,
            properties=properties
        )
