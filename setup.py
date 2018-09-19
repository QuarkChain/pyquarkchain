import os

from setuptools import setup
from setuptools.command.develop import develop

install_requires = set(x.strip() for x in open("requirements.txt"))
install_requires_replacements = {}
install_requires = [install_requires_replacements.get(r, r) for r in install_requires]
PTH = "pypy-fix-cython-warning.pth"

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...


class custom_develop(develop):
    @property
    def target(self):
        return os.path.join(self.install_dir, PTH)

    def run(self):
        print(self.install_dir)
        with open("quarkchain/tools/" + PTH) as infp:
            with open(self.target, "w") as outfp:
                outfp.write(infp.read())
        develop.run(self)


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="quarkchain",
    version="0.0",
    author="QuarkChain",
    author_email="",
    description=("QuarkChain"),
    license="MIT",
    keywords="QuarkChain,blockchain",
    url="",
    packages=["quarkchain"],
    long_description=read("README.md"),
    classifiers=["Development Status :: 0 - Development", "License :: MIT License"],
    install_requires=install_requires,
    python_requires=">=3.5",
    cmdclass={"develop": custom_develop},
)
