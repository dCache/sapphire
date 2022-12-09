Summary: dCache nearline storage provider for Sapphire
Vendor: dCache.org
Name: Sapphire
URL: https://dcache.org
Packager: dCache.org <support@dcache.org>
License: Distributable
Group: Applications/System

Version: 0.3.1
Release: 1
BuildArch: noarch
Prefix: /

AutoReqProv: no
Requires: dcache >= 7.2.2
%{?systemd_requires}

Source0: %{name}-%{version}-SNAPSHOT.tar.gz

%description
dCache Nearline storage provider on dCache side for Sapphire

%prep
%setup -q -a 0 -n %{name}-%{version}-SNAPSHOT

%install
mkdir -p %{buildroot}%{_datadir}/dcache/plugins
cp -a %{name}-%{version}-SNAPSHOT %{buildroot}%{_datadir}/dcache/plugins

%post
/usr/bin/systemctl daemon-reload >/dev/null 2>&1 ||:

%postun
/usr/bin/systemctl daemon-reload >/dev/null 2>&1 ||:

%files
%defattr(-,root,root,-)
%{_datadir}

%changelog
* Wed Dec 07 2022 Svenja Meyer <svenja.meyer@desy.de>
- Initialize package creation
