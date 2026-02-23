import sys

from airbyte_cdk.entrypoint import launch

from source_sage_intacct.source import SourceSageIntacct


if __name__ == "__main__":
    source = SourceSageIntacct()
    launch(source, sys.argv[1:])

