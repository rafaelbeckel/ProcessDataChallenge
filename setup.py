from setuptools import setup

setup(
    name='datadragon',
    version='0.1',
    py_modules=['datadragon', 
                'settings', 
                'data'],
    install_requires=[
        'Click',
        'Faker',
        'Pymongo',
        'Tblib'
    ],
    entry_points='''
        [console_scripts]
        datadragon=datadragon:main
    ''',
)