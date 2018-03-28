from distutils.core import setup

setup(
    name='gpubid',
    version='0.1',
    description='Package for all GpuBid infrastructure, including the scheduler and control tools.',
    url='https://github.com/ichaelm/gpubid/',
    packages=[
        'gpubid',
        'gpubid.tools',
    ],
    install_requires=[
        'docker',
        'flask',
        'flask_api',
    ],
)
