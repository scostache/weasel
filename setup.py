from distutils.core import setup


setup(name = "Weasel",
    version = "0-1",
    description = "MTC Platform",
    author = "Stefania Costache",
    author_email = "stefania.costache@gmail.com",
    url = "https://gforge.inria.fr/",
    platform = "Linux Debian",
    #Name the folder where your packages live:
    #(If you have other packages (dirs) or modules (py files) then
    #put them into the package directory - they will be found 
    #recursively.)
    packages = ['weasel', 
		'weasel.utils', 
		'weasel.resourcemng', \
		'weasel.etc', \
		'weasel.datastore', \
		'weasel.client', \
		'weasel.worker']
    data_files=[],
    install_requires=[],
    #'package' package must contain files (see list above)
    #I called the package 'package' thus cleverly confusing the whole issue...
    #This dict maps the package name =to=> directories
    #It says, package *needs* these files.
    #'running scripts' is in the root.
    scripts = ["bin/client", \
		"bin/resourcemng", \
		"bin/local_resourcemng"],
   long_description = """ Weasel is a set of tools to manage Many Tasks Computing applications on clusters and clouds """
)
