import pathlib
import shutil
import os
import subprocess
import setuptools
import itertools
from setuptools.command import build
from grpc_tools import command as grpc_command


_ROOT_DIR = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))

_GEN_BQ_SCHEMA_PKG = (
    'github.com/GoogleCloudPlatform/protoc-gen-bq-schema@v1.1.0'
)


class InstallProtoFilesCommand(setuptools.Command):
    """Command to install dependency proto files to the build tree."""

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        _ = subprocess.run(['go', 'install', _GEN_BQ_SCHEMA_PKG], check=True)
        p = subprocess.run(
            ['go', 'env', 'GOPATH'], capture_output=True, check=True
        )
        gopath = p.stdout.decode().strip()
        gen_bq_schema_protos = [
            (
                pathlib.Path(path)
                / 'pkg/mod/github.com/!google!cloud!platform/'
            ).glob('protoc-gen-bq-schema*/*.proto')
            for path in gopath.split(':')
        ]
        gen_bq_schema_dir = pathlib.Path(_ROOT_DIR) / 'gen_bq_schema'
        gen_bq_schema_dir.mkdir(exist_ok=True)
        for proto in itertools.chain.from_iterable(gen_bq_schema_protos):
            shutil.copy(proto, gen_bq_schema_dir)


class ExtendedBuild(build.build):
    sub_commands = [
        ('build_install_proto_files', None),
        ('build_protoc', None),
    ] + build.build.sub_commands


setuptools.setup(
    cmdclass={
        'build': ExtendedBuild,
        'build_install_proto_files': InstallProtoFilesCommand,
        'build_protoc': grpc_command.BuildPackageProtos,
    }
)
