#!/bin/sh
make clean ; make -j 12 ; cd build ; makeglossaries matter ; rm *.pdf ; cd .. ; make -j 12
