import click

@click.group()
def cli():
    """Glow cli tool."""
    click.echo('Hello World!')

@cli.command()
def fetch():
    """Fetch your definitions and store them into YML files.
    The final result will be available in your $(definitions-path) folder as defined in the glow_project.yml file."""
    click.echo('Compiling glow project')

@cli.command()
def compile():
    """Compile your YAML definitions into MD files.
    The final result will be available in your $(docs-path) folder as defined in the glow_project.yml file."""
    click.echo('Compiling glow project')

@cli.command()
def build():
    """Build your Glow project into a static html site.
    The final result will be available in your $(site-path) folder as defined in the glow_project.yml file."""
    click.echo('Building glow project')

@cli.command()
def serve():
    """Serve your latest build via a lightweigth server. (Non-PROD only!)"""
    click.echo('Serving glow project')

if __name__ == '__main__':
    cli()