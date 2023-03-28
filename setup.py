from os.path import join, dirname

from setuptools import setup

setup(
    name="fake-s3",
    version="1.0.0",

    description='A python port of Fake-S3.',
    long_description=open(join(dirname(__file__), 'README.md')).read(),

    author='RuslanUC',

    url='https://github.com/RuslanUC/Fake-S3',
    repository='https://github.com/RuslanUC/Fake-S3',
    license="MIT",

    classifiers=[
        'Operating System :: OS Independent',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Intended Audience :: Developers',
        'Environment :: Console',
    ],
    platforms=['Any'],
    install_requires=open(join(dirname(__file__), 'requirements.txt')).read().splitlines(False),
    python_requires='>=3.7',

    namespace_packages=[],
    packages=["fake_s3"],
    include_package_data=True,

    entry_points={
        'console_scripts': [
            'fake_s3 = fake_s3.main:main'
        ]
    }
)
