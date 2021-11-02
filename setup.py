from setuptools import setup
import versioneer

requirements = [
    # package requirements go here
]

setup(
    name='usgs-maps',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="USGS NWIS Maps and Data ",
    license="MIT",
    author="Nicky Sandhu",
    author_email='psandhu@water.ca.gov',
    url='https://github.com/dwr-psandhu/usgs-maps',
    packages=['usgs_maps'],
    entry_points={
        'console_scripts': [
            'usgs_maps=usgs_maps.cli:cli'
        ]
    },
    install_requires=requirements,
    keywords='usgs-maps',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
