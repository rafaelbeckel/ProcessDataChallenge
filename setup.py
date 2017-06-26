from setuptools import setup, find_packages

setup(
    name='datadragon',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'Click',
        'Faker',
        'Pymongo'
    ],
    entry_points='''
        [console_scripts]
        datadragon=datadragon:main
    ''',
)