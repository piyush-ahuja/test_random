def process_file(input_file, output_file, index_col, batch_size=250000):
    try:
        # Get total number of rows in the input file
        total_rows = pq.read_metadata(input_file).num_rows
        logging.info(f"Total rows: {total_rows}")

        # Calculate number of batches to process
        num_batches = (total_rows // batch_size) + (1 if total_rows % batch_size > 0 else 0)

        # Iterate through batches and write the index column data
        for batch_num in range(num_batches):
            start_row = batch_num * batch_size
            end_row = min((batch_num + 1) * batch_size, total_rows)
            logging.info(f"Processing batch {batch_num + 1} of {num_batches} (rows {start_row} to {end_row})")

            # Read the current batch of rows from the input file
            data = pq.read_table(input_file, skip_rows=start_row, num_rows=batch_size).to_pandas()

            # Select only the index column
            data = data[[index_col]]

            # Write the index column data to the output file
            if not data.empty:
                fp_write(output_file, data, compression='SNAPPY', append=batch_num > 0, write_options=dict(compression='snappy'))

        logging.info(f"Successfully created output file: {output_file}")

    except Exception as e:
        logging.error(f"Error processing file: {str(e)}")
        raise
