"""setup.py file for setuptools."""

import pathlib
import shutil
import os
import subprocess
import setuptools
import itertools
from setuptools.command import build
from grpc_tools import command as grpc_command
from grpc_tools import protoc
from importlib import resources
import logging


_ROOT_DIR = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))

_GEN_BQ_SCHEMA_PKG = (
    'github.com/GoogleCloudPlatform/protoc-gen-bq-schema@v1.1.0'
)


def _build_json_schemas(
    package_root: str, gen_bq_schema_command: pathlib.Path  # pylint: disable=unused-argument
) -> None:
  inclusion_root = str(pathlib.Path(package_root).resolve())  # pylint: disable=unused-variable
  proto_files = list(pathlib.Path(package_root).glob('**/*.proto'))

  well_known_protos_include = str(  # pylint: disable=unused-variable
      (resources.files('grpc_tools') / '_proto').resolve()
  )

  for proto_file in proto_files:
    command = [
        'grpc_tools.protoc',
        '--plugin={gen_bq_schema_command}',
        '--proto_path={inclusion_root}',
        '--proto_path={well_known_protos_include}',
        '--bq-schema_out={inclusion_root}',
    ] + [str(proto_file.resolve())]
    if protoc.main(command) != 0:
      logging.warning('%s failed', command)


class _GenerateBigQuerySchemas(setuptools.Command):
  """Command to generate BigQuery schemas from proto files."""

  def initialize_options(self):
    pass

  def finalize_options(self):
    pass

  def run(self):
    p = subprocess.run(['go', 'env', 'GOPATH'], capture_output=True, check=True)
    gopath = p.stdout.decode().strip()
    gen_bq_schema_command = next(
        itertools.chain.from_iterable(
            [
                (pathlib.Path(path) / 'bin').glob('protoc-gen-bq-schema')
                for path in gopath.split(':')
            ]
        ),
        None,
    )
    if not gen_bq_schema_command:
      raise OSError(
          'Missing protoc-gen-bq-schema command. '
          'Please ensure InstallProtoFilesCommand is run first.'
      )
    _build_json_schemas(
        self.distribution.package_dir[''], gen_bq_schema_command
    )


class _InstallProtoFilesCommand(setuptools.Command):
  """Command to install dependency proto files to the build tree."""

  def initialize_options(self):
    pass

  def finalize_options(self):
    pass

  def run(self):
    _ = subprocess.run(['go', 'install', _GEN_BQ_SCHEMA_PKG], check=True)
    p = subprocess.run(['go', 'env', 'GOPATH'], capture_output=True, check=True)
    gopath = p.stdout.decode().strip()
    gen_bq_schema_protos = [
        (
            pathlib.Path(path) / 'pkg/mod/github.com/!google!cloud!platform/'
        ).glob('protoc-gen-bq-schema*/*.proto')
        for path in gopath.split(':')
    ]
    gen_bq_schema_dir = pathlib.Path(_ROOT_DIR) / 'gen_bq_schema'
    shutil.rmtree(gen_bq_schema_dir, ignore_errors=True)
    gen_bq_schema_dir.mkdir()
    for proto in itertools.chain.from_iterable(gen_bq_schema_protos):
      shutil.copy(proto, gen_bq_schema_dir)


class ExtendedBuild(build.build):
  sub_commands = [
      ('build_install_proto_files', None),
      ('build_protoc', None),
      ('build_bq_schemas', None),
  ] + build.build.sub_commands


setuptools.setup(
    cmdclass={
        'build': ExtendedBuild,
        'build_install_proto_files': _InstallProtoFilesCommand,
        'build_protoc': grpc_command.BuildPackageProtos,
        'build_bq_schemas': _GenerateBigQuerySchemas,
    }
)
