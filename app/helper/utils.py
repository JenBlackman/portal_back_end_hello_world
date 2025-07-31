import os

def get_output_dir():
    # Check if running inside AWS Lambda
    if os.environ.get("AWS_EXECUTION_ENV", "").startswith("AWS_Lambda"):
        return "/tmp/output"
    else:
        return os.path.join(os.getcwd(), "output")


def get_input_dir():
    # If running inside Lambda, use /tmp
    if os.environ.get("AWS_EXECUTION_ENV", "").startswith("AWS_Lambda"):
        return "/tmp/InputFiles/txc"
    else:
        return os.path.join(os.getcwd(), "InputFiles", "txc")
