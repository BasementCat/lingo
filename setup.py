from setuptools import setup

setup(
	name='lingo',
	version='0.2',
	description='A simple MongoDB/CouchDB ORM for Python',
	long_description='',
	classifiers=[
		'Development Status :: 3 - Alpha',
		'Intended Audience :: Developers',
		'License :: Other/Proprietary License',
		'Natural Language :: English',
		'Operating System :: OS Independent',
		'Programming Language :: Python :: 2',
		'Topic :: Database',
		'Topic :: Software Development :: Libraries'
	],
	keywords='mongo mongodb orm python couchdb',
	url='https://github.com/BasementCat/lingo',
	author='BasementCat',
	author_email='basementcat@basementcat.net',
	license='',
	packages=['lingo'],
	install_requires=['pymongo', 'couchdb'],
	test_suite='nose.collector',
	tests_require=['nose', 'pymongo', 'couchdb'],
	zip_safe=False
)