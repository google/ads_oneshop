"""setup.py file for setuptools."""

import pathlib
import shutil
import subprocess
import setuptools
from setuptools.command import build
from grpc_tools import protoc
import importlib_resources as resources
import logging
import site
from typing_extensions import override

# TODO: break out commands into its own pre-build subproject

_GEN_BQ_SCHEMA_PKG = (
    'github.com/GoogleCloudPlatform/protoc-gen-bq-schema@v1.1.0'
)


def _get_gopath() -> list[str]:
  p = subprocess.run(['go', 'env', 'GOPATH'], capture_output=True, check=True)
  return p.stdout.decode().strip().split(':')


def _build_protos(
    proto_paths: list[str],
    include_paths: list[str],
    flags: list[str],
) -> None:
  """Invoke protoc with a set of flags.

  Requires an explicit list of proto paths, since globbing all files under a
  given project directory would add virtual environment protos. This is
  undesirable, since protoc has no concept of hidden directories, and will
  silently create in-tree duplicates of hidden folders if it finds protos in
  them. At the same time, protoc was designed for a monorepo, and assumes that
  all include paths are absolute roots, so we cannot simply use the proto paths,
  since these are often scoped to subdirectories of the include paths.

  This method automatically includes well known types from protobuf and grpc.

  Lastly, the caller must specify the desired output flags, and paths to any
  additional protoc plugin binaries.

  Args:
    proto_paths: An explicit list of paths to search for proto files.
    include_paths: The import path roots for this project, not including
      well-known types.
    flags: Additional flags to pass to protoc, especially plugins and
      type-specific output flags.
  """
  include_roots = [
      str(pathlib.Path(include_path).resolve())
      for include_path in include_paths
  ]

  proto_files = []
  for proto_path in proto_paths:
    proto_files.extend(pathlib.Path(proto_path).glob('**/*.proto'))

  # grpc protos
  well_known_protos_include = str(
      (resources.files('grpc_tools') / '_proto').resolve()
  )

  # google.api protos
  site_include_path = site.getsitepackages()[0]

  for proto_file in proto_files:
    command = [
        'grpc_tools.protoc',
        *[f'--proto_path={include_root}' for include_root in include_roots],
        f'--proto_path={well_known_protos_include}',
        f'--proto_path={site_include_path}',
        *flags,
    ] + [str(proto_file.resolve())]
    if protoc.main(command) != 0:
      logging.warning('%s failed', command)


class _GeneratePythonProtos(setuptools.Command):
  """Command to generate BigQuery schemas from proto files."""

  @override
  def initialize_options(self):
    pass

  @override
  def finalize_options(self):
    pass

  @override
  def run(self):
    package_paths = [
        self.distribution.package_dir[k]
        for k in self.distribution.package_dir
        if k
    ]
    include_paths = [self.distribution.package_dir['']]

    # cwd is fine here
    out_dir = str(pathlib.Path().resolve())

    output_flags = [
        f'--python_out={out_dir}',
        f'--pyi_out={out_dir}',
        f'--grpc_python_out={out_dir}',
    ]

    _build_protos(package_paths, include_paths, output_flags)


class _GenerateBigQuerySchemas(setuptools.Command):
  """Command to generate BigQuery schemas from proto files."""

  @override
  def initialize_options(self):
    pass

  @override
  def finalize_options(self):
    pass

  @override
  def run(self):
    gen_bq_schema_command = None
    for path in _get_gopath():
      for target in (pathlib.Path(path) / 'bin').glob('protoc-gen-bq-schema'):
        gen_bq_schema_command = target
        break
    if not gen_bq_schema_command:
      raise OSError(
          'Missing protoc-gen-bq-schema command. '
          'Please ensure InstallProtoFilesCommand is run first.'
      )

    package_paths = [
        self.distribution.package_dir[k]
        for k in self.distribution.package_dir
        if k
    ]
    include_paths = [self.distribution.package_dir['']]

    # cwd is fine here
    out_dir = str(pathlib.Path().resolve())

    flags = [
        f'--plugin={gen_bq_schema_command}',
        f'--bq-schema_out={out_dir}',
    ]

    _build_protos(
        package_paths,
        include_paths,
        flags,
    )


class _InstallProtoFilesCommand(setuptools.Command):
  """Command to vendor protos into the sdist or working directory."""

  @override
  def initialize_options(self):
    pass

  @override
  def finalize_options(self):
    pass

  @override
  def run(self):
    _ = subprocess.run(['go', 'install', _GEN_BQ_SCHEMA_PKG], check=True)
    gen_bq_schema_protos: list[str] = []
    for path in _get_gopath():
      for proto in (
          pathlib.Path(path) / 'pkg/mod/github.com/!google!cloud!platform/'
      ).glob('protoc-gen-bq-schema*/*.proto'):
        gen_bq_schema_protos.append(str(proto))
    gen_bq_schema_dir = (
        pathlib.Path(self.distribution.package_dir['']) / 'gen_bq_schema'
    )
    shutil.rmtree(gen_bq_schema_dir, ignore_errors=True)
    gen_bq_schema_dir.mkdir()
    for proto in gen_bq_schema_protos:
      shutil.copy(proto, gen_bq_schema_dir)


class _CopyProtosToBuildCommand(setuptools.Command):
  """Command to copy vendored protos to the bdist/wheel or working directory.

  Due to issues related to setuptools copying vendored packages, we copy files
  directly to the output directory. See:

   - https://github.com/pypa/setuptools/issues/1064
   - https://stackoverflow.com/a/75709899/14603806
  """

  @override
  def initialize_options(self):
    self.build_lib = None

  @override
  def finalize_options(self):
    self.set_undefined_options('build_py', ('build_lib', 'build_lib'))

  @override
  def run(self):
    src = pathlib.Path(self.distribution.package_dir['']) / 'gen_bq_schema'
    assert isinstance(self.build_lib, str)
    gen_bq_schema_dir = pathlib.Path(self.build_lib) / 'gen_bq_schema'
    # TODO: Omit .proto files from this copy.
    shutil.copytree(src, gen_bq_schema_dir)


class ExtendedBuild(build.build):
  """Additional subcommands for setuptools builds with protos."""

  sub_commands = [
      ('build_install_proto_files', None),
      ('build_protoc', None),
      ('build_bq_schemas', None),
      ('build_copy_protos', None),
  ] + build.build.sub_commands


setuptools.setup(
    cmdclass={
        'build': ExtendedBuild,
        'build_install_proto_files': _InstallProtoFilesCommand,
        'build_protoc': _GeneratePythonProtos,
        'build_bq_schemas': _GenerateBigQuerySchemas,
        'build_copy_protos': _CopyProtosToBuildCommand,
    }
)
