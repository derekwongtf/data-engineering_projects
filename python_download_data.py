from prefect import flow
from datetime import datetime
from prefect_shell import ShellOperation

@flow
def download_data():
    today = datetime.today().strftime("%Y%m%d")

    # for long running operations, you can use a context manager
    with ShellOperation(
        commands=[
           "./download_data.sh 2022",
        ]
    ) as download_csv_operation:

        # trigger runs the process in the background
        download_csv_process = download_csv_operation.trigger()

        # then do other things here in the meantime, like download another file
        ...

        # when you're ready, wait for the process to finish
        download_csv_process.wait_for_completion()

        # if you'd like to get the output lines, you can use the `fetch_result` method
        output_lines = download_csv_process.fetch_result()

download_data()
