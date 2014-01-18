import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
      name="pype",
      version="0.2-beta",
      author='Wessie',
      author_email='pype@wessie.info',
      description=("A lunatics pipeline"),
      license='GPL',
      install_requires=[
      ],
      dependency_links = [
      ],
      entry_points={
      },
      keywords="pipeline",
      packages=['pype'],
)
