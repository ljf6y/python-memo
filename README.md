insert_count = runner.vectorized_insert(
    upload_df,
    schema="UPLOAD",
    table_name="T456UWA0040",
    overwrite=False
)

if insert_count != len(upload_df):
    return False
else:
    return True
