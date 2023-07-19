Name:           Sapphire-Driver
Vendor:         dCache.org
Packager:       dCache.org <support@dcache.org>
Version:        0.3.1
Release:        1%{?dist}
Summary:        dCache nearline storage provider for Sapphire, driver part

License:        Distributable
URL:            https://dcache.org

Requires: dcache >= 7.2.2

# Build with following command:
# if source is located in SOURCES:
#   rpmbuild --target noarch -dd sapphire-packer.spec
# else:
#   rpmbuild --target noarch --define '_source <Path to Sapphire source>' -bb sapphire-driver.spec

%description
The driver-part of Sapphire, a plugin for dCache used to pack small files
into bigger files for improving tape perfomance

%build
SOURCE_DIR=%{_source};
if [ -z $SOURCE_DIR ]; then
    SOURCE_DIR=$RPM_SOURCE_DIR
fi;
cd $SOURCE_DIR/driver
mvn clean package
cp $SOURCE_DIR/driver/target/Sapphire-%{version}-SNAPSHOT.tar.gz $RPM_SOURCE_DIR

%install
mkdir -p %{buildroot}%{_datadir}/dcache/plugins
tar -xzvf $RPM_SOURCE_DIR/Sapphire-%{version}-SNAPSHOT.tar.gz -C $RPM_SOURCE_DIR/
cp -r $RPM_SOURCE_DIR/Sapphire-%{version}-SNAPSHOT/ %{buildroot}%{_datadir}/dcache/plugins

%post
/usr/bin/systemctl daemon-reload >/dev/null 2>&1 ||:

%postun
/usr/bin/systemctl daemon-reload >/dev/null 2>&1 ||:

%files
/usr/share/dcache/plugins/Sapphire-%{version}-SNAPSHOT/*


%changelog
* Wed Dec 07 2022 Svenja Meyer <svenja.meyer@desy.de>
- Initialize package creation
