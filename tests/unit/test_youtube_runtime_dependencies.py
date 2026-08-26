'''Tests for coupled YouTube PO-token runtime dependencies.'''

from pathlib import Path
import tomllib
import unittest


REPOSITORY_ROOT: Path = Path(__file__).resolve().parents[2]


class TestYouTubeRuntimeDependencies(unittest.TestCase):
    def test_python_po_token_provider_is_pinned(self) -> None:
        pyproject_path: Path = REPOSITORY_ROOT / 'pyproject.toml'
        with pyproject_path.open('rb') as pyproject_file:
            pyproject: dict = tomllib.load(pyproject_file)

        dependencies: list[str] = pyproject['project']['dependencies']
        self.assertIn(
            'bgutil-ytdlp-pot-provider==1.3.1',
            dependencies,
        )

    def test_provider_server_image_is_pinned(self) -> None:
        compose_path: Path = REPOSITORY_ROOT / 'docker-compose.yml'
        compose: str = compose_path.read_text(encoding='utf-8')
        self.assertIn(
            'image: brainicism/bgutil-ytdlp-pot-provider:1.3.1',
            compose,
        )


if __name__ == '__main__':
    unittest.main()
