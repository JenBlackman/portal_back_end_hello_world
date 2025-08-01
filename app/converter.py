from process_txc.read_txc import process_all_xml
from process_txc import transform
from generate_outputs import output_hastus

def run_conversion(input_dir: str, output_dir: str, base_path) -> list[str]:
    txc_tables_static = process_all_xml(input_dir)
    transformed_tables = transform.transform_all_txc_tables(txc_tables_static, base_path=base_path)
    return output_hastus.create_outputs(transformed_tables, output_dir)


# 🧪 For local testing only
if __name__ == '__main__':
    import os
    from helper.utils import get_input_dir, get_output_dir
    input_dir = get_input_dir()
    output_dir = get_output_dir()
    base_path = os.getenv("LAMBDA_TASK_ROOT", os.getcwd())
    os.makedirs(output_dir, exist_ok=True)
    run_conversion(input_dir, output_dir, base_path=base_path)
