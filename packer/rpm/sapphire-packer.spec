Name:           Sapphire-Packer
Vendor:         dCache.org
Packager:       dCache.org <support@dcache.org>
Version:        0.3.1
Release:        1%{?dist}
Summary:        dCache nearline storage provider for Sapphire, packer part

License:        Distributable
URL:            https://dcache.org

Requires: python3

# Build with following command:
# if source is located in SOURCES:
#   rpmbuild --target noarch -dd sapphire-packer.spec
# else:
#   rpmbuild --target noarch --define '_source <Path to Sapphire source>' -bb sapphire-packer.spec

%description
The packer-part of Sapphire, a plugin for dCache used to pack small files
into bigger files for improving tape perfomance

%clean
rm -rf $RPM_BUILD_ROOT/usr/local/bin
rm -rf $RPM_BUILD_ROOT/etc/dcache

%install
mkdir -p $RPM_BUILD_ROOT/usr/local/bin
mkdir -p $RPM_BUILD_ROOT/etc/systemd/system
mkdir -p $RPM_BUILD_ROOT/etc/dcache/
SOURCE_DIR=%{_source};
if [ -z $SOURCE_DIR ]; then
    SOURCE_DIR=$RPM_SOURCE_DIR
fi;
cp $SOURCE_DIR/packer/src/* $RPM_BUILD_ROOT/usr/local/bin
cp $SOURCE_DIR/packer/service/* $RPM_BUILD_ROOT/etc/systemd/system
cp $SOURCE_DIR/packer/conf/container.conf $RPM_BUILD_ROOT/etc/dcache/container.conf
ls $RPM_BUILD_ROOT/usr/local/bin


%files
%attr(0544, root, root) /usr/local/bin/*
%attr(0644, root, root) /etc/systemd/system/pack-files.service
%attr(0644, root, root) /etc/systemd/system/verify-container.service
%attr(0644, root, root) /etc/systemd/system/stage-files.service
%config(noreplace) %attr(0664, root, root) /etc/dcache/*

%postun
if [ -f "/etc/dcache/container.conf.save" ]; then
    rm /etc/dcache/container.conf.save;
fi;

%changelog
* Wed Jun 28 2023 Svenja Meyer <svenja.meyer@desy.de>
- initial version