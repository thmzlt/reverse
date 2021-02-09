#!/usr/bin/env sh

PARALLELISM=8

find data/*.data | parallel --will-cite -j $PARALLELISM "tac {} | rev > {}.reversed"
