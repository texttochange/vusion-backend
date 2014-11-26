#!/bin/sh -e

#cd "$(dirname "$0")/.."

echo "=== Nuking old .pyc files..."
find vusion/ -name '*.pyc' -delete
find transports/ -name '*.pyc' -delete
find tests/ -name '*.pyc' -delete
find dispatchers/ -name '*.pyc' -delete
find components/ -name '*.pyc' -delete
echo "=== Erasing previous coverage data..."
coverage erase
echo "=== Running trial tests..."
coverage run  `which trial` --reporter=subunit middlewares dispatchers transports vusion | subunit-1to2 | tee results.txt | subunit2pyunit
subunit2junitxml <results.txt >test_results.xml
rm results.txt
echo "=== Processing coverage data..."
coverage xml
coverage html
echo "=== Checking for PEP-8 violations..."
rm pep8.txt
pep8 --repeat --exclude='migrations' vusion transports dispatchers middlewares tests | tee pep8.txt
echo "=== Done."
