from distutils.core import setup
setup(
  name='pipelib',
  packages=['pipelib'],
  version='0.1',
  license='MIT',
  description='Helper for parallelizing map-like functions when there are generators involved.',
  author='Jordi Armengol-Estapé',
  author_email='jordi.armengol.estape@gmail.com',
  url='https://github.com/jordiae/pipelib',
  zip_safe=False,
  keywords=['multiprocessing', 'generators'],
  install_requires=[
          'multiprocessing-logging==0.3.1'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
  ],
)
