from fastparquet import write as fp_write

def create_subsets(keys_file, input_files, output_files, batch_size=250000):
    try:
        logging.info(f"Reading keys from file: {keys_file}")
        keys_data = pd.read_parquet(keys_file)
        keys_set = set(keys_data['cpf'])

        for input_file, output_file in zip(input_files, output_files):
            logging.info(f"Creating subset for file: {input_file}")

            # Get total number of rows in the input file
            total_rows = pq.read_metadata(input_file).num_rows
            logging.info(f"Total rows: {total_rows}")

            # Calculate number of batches to process
            num_batches = (total_rows // batch_size) + (1 if total_rows % batch_size > 0 else 0)

            # Iterate through batches and write the records containing cpf values found in keys_vivo.pq
            for batch_num in range(num_batches):
                start_row = batch_num * batch_size
                end_row = min((batch_num + 1) * batch_size, total_rows)
                logging.info(f"Processing batch {batch_num + 1} of {num_batches} (rows {start_row} to {end_row})")

                # Read the current batch of rows from the input file
                data = pq.read_table(input_file, skip_rows=start_row, num_rows=batch_size).to_pandas()

                # Filter the records containing cpf values found in keys_vivo.pq
                filtered_data = data[data['cpf'].isin(keys_set)]

                # Write the filtered records to the output file
                if not filtered_data.empty:
                    fp_write(output_file, filtered_data, compression='SNAPPY', append=batch_num > 0, write_options=dict(compression='snappy'))

            logging.info(f"Successfully created subset for file: {input_file}")

    except Exception as e:
        logging.error(f"Error creating subsets: {str(e)}")
        raise
