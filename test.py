from pyarrow.parquet import ParquetDataset

def process_file(input_file, output_file, index_col, batch_size=250000):
    try:
        # Get the ParquetDataset and total number of row groups in the input file
        dataset = ParquetDataset(input_file)
        total_row_groups = len(dataset.pieces)
        logging.info(f"Total row groups: {total_row_groups}")

        # Calculate number of batches to process
        num_batches = (total_row_groups // batch_size) + (1 if total_row_groups % batch_size > 0 else 0)

        # Iterate through batches and write the index column data
        for batch_num in range(num_batches):
            start_row_group = batch_num * batch_size
            end_row_group = min((batch_num + 1) * batch_size, total_row_groups)
            logging.info(f"Processing batch {batch_num + 1} of {num_batches} (row groups {start_row_group} to {end_row_group})")

            # Read the current batch of row groups from the input file
            data = dataset.read(columns=[index_col], row_groups=range(start_row_group, end_row_group)).to_pandas()

            # Write the index column data to the output file
            if not data.empty:
                fp_write(output_file, data, compression='SNAPPY', append=batch_num > 0, write_options=dict(compression='snappy'))

        logging.info(f"Successfully created output file: {output_file}")

    except Exception as e:
        logging.error(f"Error processing file: {str(e)}")
        raise
