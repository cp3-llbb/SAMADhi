# no shebang, must be sourced

# Creates a (symlinked) python install directory for SAMADhi and sets up environment variables,
# such that the `from cp3_llbb.SAMADhi.SAMADhi ...` imports can also be used standalone on ingrid.
# The python interpreter to use and the install path can be set through options

## deduce source location from the script name
if [[ -z "${ZSH_NAME}" ]]; then
  thisscript="$(readlink -f ${BASH_SOURCE})"
else
  thisscript="$(readlink -f ${0})"
fi
samadhipath="$(dirname ${thisscript})"

## option defaults
installpath="${samadhipath}/install"
python="$(which python)"
custom_python=""
## parse options
tmp_opts="$(getopt --longoptions=install:,python:,help --options=h -- $@)"
eval set -- "${tmp_opts}"
while true; do
  case "${1}" in
    --install)
      installpath="${2}"
      shift 2 ;;
    --python)
      python="${2}"
      custom_python="yes"
      shift 2 ;;
    -h|--help)
      echo "Usage: source install_standalone.sh [ --python=path_to_python_interpreter --install=./install ]"
      shift
      return 0 ;;
    --)
      shift; break ;;
  esac
done

echo "--> Install path: ${installpath}"
## prepend if necessary
function checkAndPrepend()
{
  local in_path=""
  if [[ -z "${ZSH_NAME}" ]]; then
    ## bash version
    IFS=: local exp_path=${!1}
    for apath in ${exp_path}; do
      if [[ "${apath}" == "${2}" ]]; then
        in_path="yes"
      fi
    done
  else
    ## zsh version
    local exp_path="${(P)1}"
    for apath in ${(s.:.)exp_path}; do
      if [[ "${apath}" == "${2}" ]]; then
        in_path="yes"
      fi
    done
  fi
  if [[ -z "${in_path}" ]]; then
    export ${1}="${2}:${exp_path}"
    echo "--> Added ${2} to ${1}"
  fi
}
## pick up python interpreter
if [[ -n "${custom_python}" ]]; then
  echo "--> Using python from ${python}"
  pyinterpbindir="$(dirname ${python})"
  pyinterprootdir="$(dirname ${pyinterpbindir})"
  pyinterplibdir="${pyinterprootdir}/lib"
  pyinterpsitedir="${pyinterplibdir}/python2.7/site-packages"
  checkAndPrepend "LD_LIBRARY_PATH" "${pyinterplibdir}"
  checkAndPrepend "PYTHONPATH" "${pyinterpsitedir}"
fi
pymajmin=$(${python} -c 'import sys; print(".".join(str(num) for num in sys.version_info[:2]))')
if [[ "${pymajmin}" != "2.7" ]]; then
  echo "--> Only python 2.7 is supported, please pass a suitable interpreter using the --python option (found version ${pymajmin} for ${python})"
  return 1
fi
## install upgraded pip
if [[ ! -d "${installpath}" ]]; then
  mkdir -p "${installpath}"
  echo "--> upgrading pip from $(${python} -m pip --version)"
  ${python} -m pip install --prefix="${installpath}" -I pip
fi
checkAndPrepend "LD_LIBRARY_PATH" "${installpath}/lib"
checkAndPrepend "LD_LIBRARY_PATH" "${installpath}/lib64"
pysitedir="${installpath}/lib/python${pymajmin}/site-packages"
checkAndPrepend "PYTHONPATH" "${pysitedir}"
checkAndPrepend "PYTHONPATH" "${installpath}/lib64/python${pymajmin}/site-packages"
( ${python} -c "import MySQLdb" > /dev/null 2> /dev/null ) || ${python} -m pip install --prefix="${installpath}" MySQL-python
( ${python} -c "import storm"   > /dev/null 2> /dev/null ) || ${python} -m pip install --prefix="${installpath}" storm
( ${python} -c "import ROOT"   > /dev/null 2> /dev/null ) || ${python} -m pip install --prefix="${installpath}" storm

## Install SAMADhi
if [[ ! -d "${pysitedir}/cp3_llbb" ]]; then
  mkdir -p "${pysitedir}/cp3_llbb/"
fi
## __init__.py for cp3_llbb
hatinitpy="${pysitedir}/cp3_llbb/__init__.py"
if [[ ! -f "${hatinitpy}" ]]; then
  echo "" > "${hatinitpy}"
fi
## symlink
installpy="${pysitedir}/cp3_llbb/SAMADhi"
if [[ ! -a "${installpy}" ]]; then
  ln -s "${samadhipath}/python" "${installpy}"
  echo "--> Created symlink to SAMADhi"
elif [[ ! ( -L "${installpy}" ) ]]; then
  echo "--> ${installpy} exists, but is not a symlink"
  return 1
fi
## __init__.py for cp3_llbb/SAMADhi
pkginitpy="${installpy}/__init__.py"
if [[ ! -f "${pkginitpy}" ]]; then
  echo "" > "${pkginitpy}"
fi
