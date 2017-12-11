import click
from prompt_toolkit import prompt
import subprocess as sp
import logging
import sys
import re

from ..automatic_processing.database import (
    database, setup_database, Jar, XML
)
from ..utils import load_config

log = logging.getLogger()
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler())


@click.group()
def main():
    '''
    Upload jar and xml files into the automatic processing database
    '''
    pass


@click.command()
@click.option('--config', help='Yaml file containing database credentials')
@click.option('--fact-tools-version', '-f', help='The FACT-Tools version, the xml is meant for', prompt=True)
@click.option('--name', '-n', help='The name for the xml', prompt=True)
@click.option('--comment', '-c', help='A comment to describe the xml')
@click.argument('xml-file', type=click.Path(exists=True, dir_okay=False))
def xml(config, fact_tools_version, name, comment, xml_file):
    '''
    Upload a xml into the automatic processing database

    If you do not provide name and fact-tools version on the commandline,
    you will be promptet for it.

    ARGUMENTS:
        XML_FILE path to the xml file you want to upload
    '''

    config = load_config(config)

    log.debug('Connecting to database')
    database.init(**config['processing_database'])
    database.connect()
    log.info('Database connection established')

    setup_database(database)

    try:
        jar = (
            Jar
            .select(Jar.id)
            .where(Jar.version == fact_tools_version)
            .get()
        )
    except Jar.DoesNotExist:
        database.close()
        log.error(
            'No database entry for FACT Tools version {}'.format(fact_tools_version)
        )
        sys.exit(1)

    database.close()

    with open(xml_file) as f:
        xml_content = f.read()

    if comment is None:
        comment = prompt(
            'Enter a comment for the xml: (alt-enter to finish)\n',
            multiline=True,
        )

    database.connect()
    xml = XML(
        content=xml_content,
        jar=jar,
        name=name,
        comment=comment,
    )
    xml.save()


@click.command()
@click.option('--config', help='Yaml file containing database credentials')
@click.option('--version', help='Set the version, if not the jar is executed to obtain it')
@click.argument('jar-file')
def jar(config, version, jar_file):
    '''
    Upload a FACT-Tools jar into the automatic processing database

    For FACT-Tools versions prior to 0.19.0, you should provide a version
    string on the commandline using the `--version` option

    ARGUMENTS:
        JAR_FILE path to the JAR file you want to upload
    '''

    config = load_config(config)

    log.debug('Connecting to database')
    database.init(**config['processing_database'])

    output = sp.check_output(['java', '-jar', jar_file, '--version']).decode()

    if version is None:
        version_info = {}
        for line in output.splitlines():
            k, value = line.split(':')
            version_info[k.strip()] = value.strip()

        try:
            version = version_info['git description']
        except KeyError:
            click.echo(
                'Jar did not report version, please use the `--version` option'
            )
            raise click.Abort()
        log.info('Found FACT Tools version "{}"'.format(version))
    else:
        log.info('Using version from the command line: "{}"'.format(version))

    with open(jar_file, 'rb') as f:
        jarblob = f.read()

    database.connect()
    log.info('Database connection established')
    setup_database(database)
    Jar.create(version=version, data=jarblob)
    database.close()


main.add_command(jar)
main.add_command(xml)


if __name__ == '__main__':
    main()
