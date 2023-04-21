from fastparquet import write as fp_write

from pyarrow.parquet import ParquetDataset

def create_subsets(keys_file, input_files, output_files, batch_size=250000):
    try:
        logging.info(f"Reading keys from file: {keys_file}")
        keys_data = pd.read_parquet(keys_file)
        keys_set = set(keys_data['cpf'])

        for input_file, output_file in zip(input_files, output_files):
            logging.info(f"Creating subset for file: {input_file}")

            # Get the ParquetDataset and total number of row groups in the input file
            dataset = ParquetDataset(input_file)
            total_row_groups = len(dataset.pieces)
            logging.info(f"Total row groups: {total_row_groups}")

            # Calculate number of batches to process
            num_batches = (total_row_groups // batch_size) + (1 if total_row_groups % batch_size > 0 else 0)

            # Iterate through batches and write the records containing cpf values found in keys_vivo.pq
            for batch_num in range(num_batches):
                start_row_group = batch_num * batch_size
                end_row_group = min((batch_num + 1) * batch_size, total_row_groups)
                logging.info(f"Processing batch {batch_num + 1} of {num_batches} (row groups {start_row_group} to {end_row_group})")

                # Read the current batch of row groups from the input file
                data = dataset.read(columns=['cpf'], row_groups=range(start_row_group, end_row_group)).to_pandas()

                # Filter the records containing cpf values found in keys_vivo.pq
                filtered_data = data[data['cpf'].isin(keys_set)]

                # Write the filtered records to the output file
                if not filtered_data.empty:
                    fp_write(output_file, filtered_data, compression='SNAPPY', append=batch_num > 0, write_options=dict(compression='snappy'))

            logging.info(f"Successfully created subset for file: {input_file}")

    except Exception as e:
        logging.error(f"Error creating subsets: {str(e)}")
        raise
