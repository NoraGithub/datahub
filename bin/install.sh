#!/bin/sh
set -e
#
# This script is meant for quick & easy install via:
#   'curl -sSL https://hub.dataos.io/install.sh | sh -s [DatahubToken]'

# or:
#   'wget -qO- https://hub.dataos.io/install.sh | sh -s [DatahubToken]'


if [ -z "$1" ]
then
    echo 'Error: you should install datahub with daemonid like:'
    echo 'curl -sSL http://hub.dataos.io/install.sh | sh -s [DatahubDaemonID]'
    exit 0
fi

daemonid=$1

url='http://hub.dataos.io'

deb_package='datahub_0.8.0-1_amd64.deb'
rpm_x86_64_package='datahub-0.8.0-1.x86_64.rpm'

command_exists() {
	command -v "$@" > /dev/null 2>&1
}

case "$(uname -m)" in
	*64)
		;;
	*)
		echo >&2 'Error: you are not using a 64bit platform.'
		echo >&2 'datahub currently only supports 64bit platforms.'
		exit 1
		;;
esac


user="$(id -un 2>/dev/null || true)"

sh_c='sh -c'
if [ "$user" != 'root' ]; then
	if command_exists sudo; then
		sh_c='sudo -E sh -c'
	elif command_exists su; then
		sh_c='su -c'
	else
		echo >&2 'Error: this installer needs the ability to run commands as root.'
		echo >&2 'We are unable to find either "sudo" or "su" available to make this happen.'
	fi
fi

curl=''
if command_exists curl; then
	curl='curl --retry 20 --retry-delay 5 -L'
else
	echo >&2 'Error: this installer needs curl. You should install curl first.'
	exit 1
fi

check_datahub() {
	 
	if ps ax | grep -v grep | grep "datahub" > /dev/null
	then
	    echo "datahub daemon is running, you don't need to reinstall it. Otherwise stop it first."
	    #if command_exists service; then
	    #	$sh_c "service datahub stop"
	    #fi
	fi
	 
}

#check_datahub


# perform some very rudimentary platform detection
lsb_dist=''
if command_exists lsb_release; then
	lsb_dist="$(lsb_release -si)"
fi
if [ -z "$lsb_dist" ] && [ -r /etc/lsb-release ]; then
	lsb_dist="$(. /etc/lsb-release && echo "$DISTRIB_ID")"
fi
if [ -z "$lsb_dist" ] && [ -r /etc/debian_version ]; then
	lsb_dist='debian'
fi
if [ -z "$lsb_dist" ] && [ -r /etc/fedora-release ]; then
	lsb_dist='fedora'
fi
if [ -z "$lsb_dist" ] && [ -r /etc/os-release ]; then
	lsb_dist="$(. /etc/os-release && echo "$ID")"
fi
if [ -z "$lsb_dist" ] && [ -r /etc/centos-release ]; then
	lsb_dist="$(cat /etc/*-release | head -n1 | cut -d " " -f1)"
fi
if [ -z "$lsb_dist" ] && [ -r /etc/redhat-release ]; then
	lsb_dist="$(cat /etc/*-release | head -n1 | cut -d " " -f1)"
fi
lsb_dist="$(echo $lsb_dist | cut -d " " -f1)"

lsb_version=""
if [ -r /etc/os-release ]; then
	lsb_version="$(. /etc/os-release && echo "$VERSION_ID")"
fi

start_datahub() {

	echo " * Start datahub daemon..."
    $sh_c "datahub stop"
    sleep 1
	$sh_c "datahub --daemon --token=$daemonid"

	if command_exists /usr/bin/datahub; then
		
		echo
		echo "You can view datahub daemon log at /var/log/datahub.log"
		#echo "And You can Start or Stop daomonit with: service daomonit start/stop/restart/status"
		echo

		echo "*********************************************************************"
		echo "***"
		echo "***  Installed and Started Datahub Client Daemon $(/usr/bin/datahub version)"
		echo "***"
		echo "*********************************************************************"
		echo
		
	fi
}

not_support () {
	echo
	echo "datahub not support ${lsb_dist} ${lsb_version} now"
	echo
	exit 0
}


lsb_dist="$(echo "$lsb_dist" | tr '[:upper:]' '[:lower:]')"
case "$lsb_dist" in
	fedora|centos|amzn)
		(
            echo " * Installing datahub..."
			echo " * Downloading datahub from ${url}/${rpm_x86_64_package}"
			$sh_c "$curl -o /tmp/${rpm_x86_64_package} ${url}/${rpm_x86_64_package}"	
			if command_exists /usr/bin/datahub; then
                echo "uninstall old version of datahub"
                                $sh_c "datahub stop"
				sleep 1
				$sh_c "rm /usr/bin/datahub"

                        fi                      
                        $sh_c "rpm -ivh /tmp/${rpm_x86_64_package}"
            $sh_c "rm -rf /tmp/${rpm_x86_64_package}"
			start_datahub
		)
		exit 0
		;;

	ubuntu|debian)
		(

			echo " * Installing datahub..."

			echo " * Downloading datahub from ${url}/${deb_package}"
			$sh_c "$curl -o /tmp/${deb_package} ${url}/${deb_package}"
			if command_exists /usr/bin/datahub; then
                echo "uninstall old version of datahub"
				$sh_c "dpkg -r datahub"
			fi			
			$sh_c "dpkg -i /tmp/${deb_package}"
            $sh_c "rm -rf /tmp/${deb_package}"
			
			start_datahub 

		)
		exit 0
		;;

esac

echo ALL DONE && exit 0
