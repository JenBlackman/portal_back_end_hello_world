from process_txc.read_txc import process_all_xml
from process_txc import transform
from generate_outputs import output_hastus

def run_conversion(input_dir: str, output_dir: str) -> list[str]:
    txc_tables_static = process_all_xml(input_dir)
    transformed_tables = transform.transform_all_txc_tables(txc_tables_static)
    return output_hastus.create_outputs(transformed_tables, output_dir)

# Optional: retain script functionality for CLI use
if __name__ == '__main__':
    import os
    from helper.utils import get_input_dir, get_output_dir
    output_dir = get_output_dir()
    os.makedirs(output_dir, exist_ok=True)
    run_conversion(get_input_dir(), get_output_dir())