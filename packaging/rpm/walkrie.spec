Name:           walkrie
Version:        1.2.0~alpha1
Release:        1%{?dist}
Summary:        PostgreSQL WAL to vector embedding sync engine

License:        Apache-2.0
URL:            https://github.com/bugraaktug/walkrie
# Built from a full source-tree snapshot with git submodules already
# vendored (see make-tarball.sh in this directory) rather than fetched
# via `git submodule update` during %prep — rpmbuild environments
# (mock/koji in particular) commonly run with no network access.
Source0:        %{name}-%{version}.tar.gz
# Kept as a separate file (like debian/changelog) rather than inlined below
# — %include pulls it in at spec-parse time, so it must already be staged
# in the SOURCES dir before rpmbuild runs (packaging/rpm/build.sh does this).
Source1:        walkrie.changelog

BuildRequires:  cmake >= 3.16
BuildRequires:  cmake-rpm-macros
BuildRequires:  gcc-c++ >= 11
BuildRequires:  libpq-devel
BuildRequires:  libevent-devel
BuildRequires:  libcurl-devel
BuildRequires:  json-devel
BuildRequires:  spdlog-devel
BuildRequires:  sqlite-devel
BuildRequires:  systemd-rpm-macros
# NOTE: RHEL/Rocky 9's sqlite-devel (3.34.x) predates the RETURNING-clause
# support (3.35.0+) that BackfillStore::claim_pending relies on for an
# atomic claim-without-race — CMakeLists.txt detects this at configure
# time and falls back to fetching and building the SQLite amalgamation
# from source instead. That fallback needs network access DURING %build
# on this platform; a fully offline/mock build needs either a system
# sqlite >= 3.35.0 or the amalgamation tarball pre-seeded into the build
# tree's _deps cache.

Requires:       libpq
Requires:       libevent
Requires:       libcurl
Requires(pre):  shadow-utils
%{?systemd_requires}

%description
Walkrie streams PostgreSQL logical replication changes into pgvector
embeddings in real time, using local (llama.cpp) or remote (OpenAI)
embedding providers. No external message broker required.

This is an early alpha release — see /usr/share/doc/walkrie/PERFORMANCE.md
for current benchmark numbers and known limitations.

%prep
%setup -q -n %{name}-%{version}

%build
%cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo \
       -DLLAMA_DIR=%{_builddir}/%{name}-%{version}/third_party/llama.cpp \
       -DBUILD_SHARED_LIBS=OFF
# Only the two binaries this package ships — skips walkrie_tests and the
# bench/demo targets CMakeLists.txt also defines but this spec doesn't
# install (see PGDG's install() rule in CMakeLists.txt).
%cmake_build --target walkrie --target walkrie_worker

%install
%cmake_install

install -Dm0644 config_sample.toml %{buildroot}%{_sysconfdir}/walkrie/config.toml
install -Dm0644 packaging/debian/walkrie.service %{buildroot}%{_unitdir}/walkrie.service
install -Dm0644 packaging/debian/walkrie.tmpfiles %{buildroot}%{_tmpfilesdir}/walkrie.conf

install -dm0750 %{buildroot}%{_sharedstatedir}/walkrie/models
install -dm0750 %{buildroot}%{_sharedstatedir}/walkrie/backfill

%pre
getent group walkrie >/dev/null || groupadd -r walkrie
getent passwd walkrie >/dev/null || \
    useradd -r -g walkrie -d %{_sharedstatedir}/walkrie -s /sbin/nologin \
            -c "Walkrie CDC daemon" walkrie
exit 0

%post
%systemd_post walkrie.service
# Contains DB credentials and possibly an API key — group-readable by
# walkrie only, not world-readable. (%attr in %files already sets this
# at install time; re-applied here so an upgrade over a manually-edited
# config doesn't leave stale ownership behind.)
chown root:walkrie %{_sysconfdir}/walkrie/config.toml 2>/dev/null || :
chmod 640 %{_sysconfdir}/walkrie/config.toml 2>/dev/null || :

%preun
%systemd_preun walkrie.service

%postun
%systemd_postun_with_restart walkrie.service
# Deliberately NOT removing the walkrie user/group or /var/lib/walkrie,
# /var/log/walkrie, /etc/walkrie here, unlike the .deb's postrm purge
# case — plain `rpm -e` has no equivalent "purge vs. remove" distinction
# to key that off, and unconditionally deleting a system user (and
# anyone's un-drained backfill state or logs) on every uninstall is
# surprising. Matches how most RPM-packaged system services (e.g.
# postgresql-server) leave their service user in place on erase.

%files
%license LICENSE
%doc README.md TECHNICAL.md PERFORMANCE.md THIRD-PARTY-LICENSES.md
%{_bindir}/walkrie
%{_bindir}/walkrie_worker
%{_datadir}/walkrie/config.toml.example
%dir %attr(0750,root,walkrie) %{_sysconfdir}/walkrie
%config(noreplace) %attr(0640,root,walkrie) %{_sysconfdir}/walkrie/config.toml
%{_unitdir}/walkrie.service
%{_tmpfilesdir}/walkrie.conf
%dir %attr(0750,walkrie,walkrie) %{_sharedstatedir}/walkrie
%dir %attr(0750,walkrie,walkrie) %{_sharedstatedir}/walkrie/models
%dir %attr(0750,walkrie,walkrie) %{_sharedstatedir}/walkrie/backfill

%changelog
%include %{SOURCE1}
