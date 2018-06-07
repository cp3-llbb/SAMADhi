#!/bin/sh
#
# Creates a (symlinked) python install directory for SAMADhi and sets up environment variables,
# such that the `from cp3_llbb.SAMADhi.SAMADhi ...` imports can also be used standalone on ingrid.
# The install directory can be configured with the PREFIX environment variable
#
## figure out where to install ($PREFIX/install_python, with default PREFIX=SAMDhi)
thisscript="$(readlink -f ${0})"
samadhipath="$(dirname ${thisscript})"
if [[ -z "${PREFIX}" ]]; then
  PREFIX="${samadhipath}"
fi
installpath="${PREFIX}/install_python"
if [[ ! -d "${installpath}" ]]; then
  echo "--> Installing into ${installpath}"
  mkdir -p "${installpath}/cp3_llbb/"
fi
## __init__.py for cp3_llbb
hatinitpy="${installpath}/cp3_llbb/__init__.py"
if [[ ! -f "${hatinitpy}" ]]; then
  echo "" > "${hatinitpy}"
fi
## symlink
installpy="${installpath}/cp3_llbb/SAMADhi"
if [[ ! -a "${installpy}" ]]; then
  ln -s "${samadhipath}/python" "${installpy}"
  echo "--> Created symlink to SAMADhi"
elif [[ ! ( -L "${installpy}" ) ]]; then
  echo "--> ${installpy} exists, but is not a symlink"
  exit 1
fi
## __init__.py for cp3_llbb/SAMADhi
pkginitpy="${installpy}/__init__.py"
if [[ ! -f "${pkginitpy}" ]]; then
  echo "" > "${pkginitpy}"
fi
## add PYTHONPATH (and LD_LIBRARY_PATH for python 2.7 if necessary)
function checkAndAdd()
{
  local bk_ifs="${IFS}"
  IFS=":"
  local in_path=""
  exp_path=$(eval echo -e "\$${1}")
  for apath in ${=exp_path}; do
    if [[ "${apath}" == "${2}" ]]; then
      in_path="yes"
    fi
  done
  IFS="${bk_ifs}"
  if [[ -z "${in_path}" ]]; then
    export ${1}="${2}:${exp_path}"
    echo "--> Added ${2} to ${1}"
  fi
}
checkAndAdd "PYTHONPATH" "${installpath}"
checkAndAdd "LD_LIBRARY_PATH" "/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/lib"
