"""
Tests for packaging metadata and release tooling.
"""

import re
import shutil
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_INIT = REPO_ROOT / "pystore" / "__init__.py"
PROJECT_COPY_IGNORE = shutil.ignore_patterns(
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    "__pycache__",
    "build",
    "dist",
    "*.egg-info",
)


def _load_package_version() -> str:
    """Load the package version without importing the runtime package graph."""
    match = re.search(
        r'^__version__\s*=\s*"([^"]+)"',
        PACKAGE_INIT.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    assert match is not None, "Could not determine pystore.__version__"
    return match.group(1)


PACKAGE_VERSION = _load_package_version()
RequirementPin = tuple[str, str, str | None]
RequirementKey = tuple[str, tuple[str, ...], str, str | None]

CORE_DEPENDENCY_PINS: tuple[RequirementPin, ...] = (
    ("pandas", "2.3.1", None),
    ("pyarrow", "21.0.0", None),
    ("dask[complete]", "2024.8.0", "python_version < '3.10'"),
    ("dask[complete]", "2025.7.0", "python_version >= '3.10'"),
    ("numpy", "1.26.4", None),
    ("fsspec", "2025.7.0", None),
    ("toolz", "1.0.0", None),
    ("cloudpickle", "3.1.1", None),
    ("python-snappy", "0.7.3", None),
    ("partd", "1.4.2", None),
)
MONITORING_DEPENDENCY_PINS: tuple[RequirementPin, ...] = (("psutil", "7.0.0", None),)
DEV_DEPENDENCY_PINS: tuple[RequirementPin, ...] = (
    ("build", "1.4.2", None),
    ("pytest", "8.4.1", None),
    ("pytest-asyncio", "1.1.0", None),
    ("pytest-cov", "6.2.1", None),
    ("pytest-xdist", "3.8.0", None),
    ("black", "25.1.0", None),
    ("ruff", "0.12.4", None),
    ("mypy", "1.17.0", None),
    ("pandas-stubs", "2.2.2.240807", "python_version < '3.10'"),
    ("pandas-stubs", "2.3.0.250703", "python_version >= '3.10'"),
    ("types-setuptools", "80.9.0.20250529", None),
)
DOCS_DEPENDENCY_PINS: tuple[RequirementPin, ...] = (
    ("sphinx", "7.4.7", "python_version < '3.10'"),
    ("sphinx", "8.1.3", "python_version == '3.10'"),
    ("sphinx", "8.2.3", "python_version >= '3.11'"),
    ("sphinx-rtd-theme", "3.0.2", None),
    ("sphinx-autodoc-typehints", "2.3.0", "python_version < '3.10'"),
    ("sphinx-autodoc-typehints", "3.0.1", "python_version == '3.10'"),
    ("sphinx-autodoc-typehints", "3.2.0", "python_version >= '3.11'"),
)
CONDA_HOST_REQUIREMENTS: tuple[str, ...] = (
    "- cloudpickle ==3.1.1",
    "- dask ==2024.8.0  # [py<310]",
    "- dask ==2025.7.0  # [py>=310]",
    "- distributed ==2024.8.0  # [py<310]",
    "- distributed ==2025.7.0  # [py>=310]",
    "- fsspec ==2025.7.0",
    "- numpy ==1.26.4",
    "- pandas ==2.3.1",
    "- partd ==1.4.2",
    "- pyarrow ==21.0.0",
    "- python-snappy ==0.7.3",
    "- toolz ==1.0.0",
)
CONDA_RUN_REQUIREMENTS: tuple[str, ...] = (
    "- cloudpickle 3.1.*",
    "- dask 2024.8.*  # [py<310]",
    "- dask 2025.7.*  # [py>=310]",
    "- distributed 2024.8.*  # [py<310]",
    "- distributed 2025.7.*  # [py>=310]",
    "- fsspec 2025.7.*",
    "- numpy 1.26.*",
    "- pandas 2.3.*",
    "- partd 1.4.*",
    "- pyarrow 21.0.*",
    "- python-snappy 0.7.*",
    "- toolz 1.0.*",
)
PYPROJECT_REQUIREMENT_PATTERN = re.compile(
    r'"([A-Za-z0-9_.-]+(?:\[[^"]+\])?[^"\n]*(?:==|>=|<=|~=|!=)[^"\n]*)"'
)


def _format_requirement_pin(pin: RequirementPin) -> str:
    name, version, marker = pin
    requirement = f"{name}=={version}"
    return f"{requirement}; {marker}" if marker else requirement


def _requirement_key(specification: str | Requirement) -> RequirementKey:
    requirement = (
        Requirement(specification)
        if isinstance(specification, str)
        else specification
    )
    marker = str(requirement.marker) if requirement.marker else None
    return (
        requirement.name.lower(),
        tuple(sorted(requirement.extras)),
        str(requirement.specifier),
        marker,
    )


def _extract_pyproject_requirement_keys(pyproject: str) -> set[RequirementKey]:
    return {
        _requirement_key(match)
        for match in PYPROJECT_REQUIREMENT_PATTERN.findall(pyproject)
    }


def _extract_requirements_keys(requirements_text: str) -> set[RequirementKey]:
    keys: set[RequirementKey] = set()
    for line in requirements_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        keys.add(_requirement_key(stripped))
    return keys


def _extract_meta_requirement_entries(recipe: str, block_name: str) -> set[str]:
    entries: set[str] = set()
    in_requirements = False
    collecting = False

    for line in recipe.splitlines():
        if line == "requirements:":
            in_requirements = True
            continue
        if in_requirements and re.match(r"^[A-Za-z_].*:$", line):
            break
        if in_requirements and line == f"  {block_name}:":
            collecting = True
            continue
        if collecting and re.match(r"^  [A-Za-z_].*:$", line):
            break
        if collecting and line.startswith("    - "):
            entries.add(line.strip())

    return entries


class TestPackagingTooling:
    """Test packaging metadata and release workflow configuration."""

    def test_dependency_manifests_pin_exact_versions(self):
        """Dependency manifests should use exact pins for direct dependencies."""
        pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        requirements = (REPO_ROOT / "requirements.txt").read_text(encoding="utf-8")
        pyproject_requirements = _extract_pyproject_requirement_keys(pyproject)
        requirements_txt_requirements = _extract_requirements_keys(requirements)
        expected_pyproject_requirements = {
            _requirement_key("setuptools==77.0.3"),
            *{
                _requirement_key(_format_requirement_pin(pin))
                for pin in (
                    CORE_DEPENDENCY_PINS
                    + MONITORING_DEPENDENCY_PINS
                    + DEV_DEPENDENCY_PINS
                    + DOCS_DEPENDENCY_PINS
                )
            },
        }
        expected_requirements_txt = {
            _requirement_key(_format_requirement_pin(pin))
            for pin in CORE_DEPENDENCY_PINS
        }

        assert 'requires-python = ">=3.9,<3.13"' in pyproject
        assert expected_pyproject_requirements <= pyproject_requirements
        assert expected_requirements_txt <= requirements_txt_requirements

        exact_pinned_packages = {
            key[0] for key in expected_pyproject_requirements | expected_requirements_txt
        }
        for name, _, specifier, _ in pyproject_requirements | requirements_txt_requirements:
            if name in exact_pinned_packages:
                assert specifier.startswith("=="), (
                    f"Expected {name!r} to remain exactly pinned, got {specifier!r}"
                )

    def test_build_outputs_include_typing_marker_and_metadata(self, tmp_path):
        """Building the project should produce distributions with expected metadata."""
        source_dir = tmp_path / "source"
        dist_dir = tmp_path / "dist"
        shutil.copytree(REPO_ROOT, source_dir, ignore=PROJECT_COPY_IGNORE)

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "build",
                "--sdist",
                "--wheel",
                "--outdir",
                str(dist_dir),
                str(source_dir),
            ],
            capture_output=True,
            check=False,
            cwd=source_dir,
            text=True,
        )

        assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"

        wheel_path = next(dist_dir.glob("pystore-*.whl"))
        sdist_path = next(dist_dir.glob("pystore-*.tar.gz"))

        with zipfile.ZipFile(wheel_path) as wheel_archive:
            wheel_names = wheel_archive.namelist()
            metadata_name = next(
                name for name in wheel_names if name.endswith(".dist-info/METADATA")
            )
            metadata = wheel_archive.read(metadata_name).decode()

        assert "pystore/py.typed" in wheel_names
        assert f"Version: {PACKAGE_VERSION}" in metadata
        requires_python = re.search(
            r"^Requires-Python: (?P<specifier>.+)$",
            metadata,
            re.MULTILINE,
        )
        assert requires_python is not None
        assert str(SpecifierSet(requires_python.group("specifier"))) == str(
            SpecifierSet(">=3.9,<3.13")
        )
        assert "License-Expression: Apache-2.0" in metadata

        with tarfile.open(sdist_path, "r:gz") as sdist_archive:
            sdist_names = sdist_archive.getnames()

        assert any(name.endswith("pystore/py.typed") for name in sdist_names)
        assert any(name.endswith("pyproject.toml") for name in sdist_names)

    def test_pyproject_uses_single_version_source(self):
        """The project metadata should source the version from the package."""
        pyproject = (REPO_ROOT / "pyproject.toml").read_text()

        assert 'dynamic = ["version"]' in pyproject
        assert 'version = {attr = "pystore.__version__"}' in pyproject
        assert "setuptools-scm" not in pyproject

    def test_conda_recipe_tracks_current_release_metadata(self):
        """The conda recipe should match the current package release metadata."""
        recipe = (REPO_ROOT / "meta.yaml").read_text(encoding="utf-8")
        host_requirements = _extract_meta_requirement_entries(recipe, "host")
        run_requirements = _extract_meta_requirement_entries(recipe, "run")

        assert '{% set name = "pystore" %}' in recipe
        assert f'{{% set version = "{PACKAGE_VERSION}" %}}' in recipe
        assert (
            "https://pypi.io/packages/source/{{ name[0] }}/{{ name }}/"
            "{{ name }}-{{ version }}.tar.gz"
        ) in recipe
        assert 'license_file: "LICENSE.txt"' in recipe
        assert "- fsspec ==2025.7.0" in recipe

        assert set(CONDA_HOST_REQUIREMENTS) <= host_requirements
        assert set(CONDA_RUN_REQUIREMENTS) <= run_requirements
        assert all(
            "==" in entry
            for entry in host_requirements
            if entry not in {"- pip", "- python >=3.9"}
        )
        assert all("==" not in entry for entry in run_requirements if entry != "- python >=3.9")

    def test_publish_workflow_uses_modern_build_and_publish_steps(self):
        """The publish workflow should use the modern PyPA build and publish path."""
        workflow = (REPO_ROOT / ".github" / "workflows" / "python-publish.yml").read_text()

        assert "types: [published]" in workflow
        assert "actions/checkout@v4" in workflow
        assert "actions/setup-python@v5" in workflow
        assert "python -m build" in workflow
        assert "python -m twine check dist/*" in workflow
        assert "actions/upload-artifact@v4" in workflow
        assert "actions/download-artifact@v4" in workflow
        assert "id-token: write" in workflow
        assert "pypa/gh-action-pypi-publish@release/v1" in workflow
        assert "setup.py sdist bdist_wheel" not in workflow

        # Build tools should use pinned versions, not --upgrade
        assert "pip install pip==25.2 build==1.4.2 twine==6.1.0" in workflow
        assert "--upgrade" not in workflow
