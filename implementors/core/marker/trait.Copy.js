(function() {var implementors = {};
implementors["aho_corasick"] = [{"text":"impl Copy for MatchKind","synthetic":false,"types":[]},{"text":"impl Copy for MatchKind","synthetic":false,"types":[]}];
implementors["ansi_term"] = [{"text":"impl Copy for Prefix","synthetic":false,"types":[]},{"text":"impl Copy for Infix","synthetic":false,"types":[]},{"text":"impl Copy for Suffix","synthetic":false,"types":[]},{"text":"impl Copy for Style","synthetic":false,"types":[]},{"text":"impl Copy for Colour","synthetic":false,"types":[]}];
implementors["arrayvec"] = [{"text":"impl&lt;A:&nbsp;Copy&gt; Copy for ArrayString&lt;A&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;A: Array&lt;Item = u8&gt; + Copy,<br>&nbsp;&nbsp;&nbsp;&nbsp;A::Index: Copy,&nbsp;</span>","synthetic":false,"types":[]},{"text":"impl&lt;T:&nbsp;Copy&gt; Copy for CapacityError&lt;T&gt;","synthetic":false,"types":[]}];
implementors["atty"] = [{"text":"impl Copy for Stream","synthetic":false,"types":[]}];
implementors["clap"] = [{"text":"impl Copy for AppSettings","synthetic":false,"types":[]},{"text":"impl Copy for ArgSettings","synthetic":false,"types":[]},{"text":"impl Copy for Shell","synthetic":false,"types":[]},{"text":"impl Copy for ErrorKind","synthetic":false,"types":[]}];
implementors["config"] = [{"text":"impl Copy for FileFormat","synthetic":false,"types":[]}];
implementors["filetime"] = [{"text":"impl Copy for FileTime","synthetic":false,"types":[]}];
implementors["ini"] = [{"text":"impl Copy for EscapePolicy","synthetic":false,"types":[]}];
implementors["inotify"] = [{"text":"impl Copy for EventMask","synthetic":false,"types":[]},{"text":"impl Copy for WatchMask","synthetic":false,"types":[]}];
implementors["inotify_sys"] = [{"text":"impl Copy for inotify_event","synthetic":false,"types":[]}];
implementors["itoa"] = [{"text":"impl Copy for Buffer","synthetic":false,"types":[]}];
implementors["lexical_core"] = [{"text":"impl Copy for ErrorCode","synthetic":false,"types":[]},{"text":"impl Copy for Error","synthetic":false,"types":[]}];
implementors["libc"] = [{"text":"impl Copy for DIR","synthetic":false,"types":[]},{"text":"impl Copy for group","synthetic":false,"types":[]},{"text":"impl Copy for utimbuf","synthetic":false,"types":[]},{"text":"impl Copy for timeval","synthetic":false,"types":[]},{"text":"impl Copy for timespec","synthetic":false,"types":[]},{"text":"impl Copy for rlimit","synthetic":false,"types":[]},{"text":"impl Copy for rusage","synthetic":false,"types":[]},{"text":"impl Copy for ipv6_mreq","synthetic":false,"types":[]},{"text":"impl Copy for hostent","synthetic":false,"types":[]},{"text":"impl Copy for iovec","synthetic":false,"types":[]},{"text":"impl Copy for pollfd","synthetic":false,"types":[]},{"text":"impl Copy for winsize","synthetic":false,"types":[]},{"text":"impl Copy for linger","synthetic":false,"types":[]},{"text":"impl Copy for sigval","synthetic":false,"types":[]},{"text":"impl Copy for itimerval","synthetic":false,"types":[]},{"text":"impl Copy for tms","synthetic":false,"types":[]},{"text":"impl Copy for servent","synthetic":false,"types":[]},{"text":"impl Copy for protoent","synthetic":false,"types":[]},{"text":"impl Copy for FILE","synthetic":false,"types":[]},{"text":"impl Copy for fpos_t","synthetic":false,"types":[]},{"text":"impl Copy for timezone","synthetic":false,"types":[]},{"text":"impl Copy for in_addr","synthetic":false,"types":[]},{"text":"impl Copy for ip_mreq","synthetic":false,"types":[]},{"text":"impl Copy for ip_mreq_source","synthetic":false,"types":[]},{"text":"impl Copy for sockaddr","synthetic":false,"types":[]},{"text":"impl Copy for sockaddr_in","synthetic":false,"types":[]},{"text":"impl Copy for sockaddr_in6","synthetic":false,"types":[]},{"text":"impl Copy for addrinfo","synthetic":false,"types":[]},{"text":"impl Copy for sockaddr_ll","synthetic":false,"types":[]},{"text":"impl Copy for fd_set","synthetic":false,"types":[]},{"text":"impl Copy for tm","synthetic":false,"types":[]},{"text":"impl Copy for sched_param","synthetic":false,"types":[]},{"text":"impl Copy for Dl_info","synthetic":false,"types":[]},{"text":"impl Copy for lconv","synthetic":false,"types":[]},{"text":"impl Copy for in_pktinfo","synthetic":false,"types":[]},{"text":"impl Copy for ifaddrs","synthetic":false,"types":[]},{"text":"impl Copy for in6_rtmsg","synthetic":false,"types":[]},{"text":"impl Copy for arpreq","synthetic":false,"types":[]},{"text":"impl Copy for arpreq_old","synthetic":false,"types":[]},{"text":"impl Copy for arphdr","synthetic":false,"types":[]},{"text":"impl Copy for mmsghdr","synthetic":false,"types":[]},{"text":"impl Copy for epoll_event","synthetic":false,"types":[]},{"text":"impl Copy for sockaddr_un","synthetic":false,"types":[]},{"text":"impl Copy for sockaddr_storage","synthetic":false,"types":[]},{"text":"impl Copy for utsname","synthetic":false,"types":[]},{"text":"impl Copy for sigevent","synthetic":false,"types":[]},{"text":"impl Copy for fpos64_t","synthetic":false,"types":[]},{"text":"impl Copy for rlimit64","synthetic":false,"types":[]},{"text":"impl Copy for glob_t","synthetic":false,"types":[]},{"text":"impl Copy for passwd","synthetic":false,"types":[]},{"text":"impl Copy for spwd","synthetic":false,"types":[]},{"text":"impl Copy for dqblk","synthetic":false,"types":[]},{"text":"impl Copy for signalfd_siginfo","synthetic":false,"types":[]},{"text":"impl Copy for itimerspec","synthetic":false,"types":[]},{"text":"impl Copy for fsid_t","synthetic":false,"types":[]},{"text":"impl Copy for packet_mreq","synthetic":false,"types":[]},{"text":"impl Copy for cpu_set_t","synthetic":false,"types":[]},{"text":"impl Copy for if_nameindex","synthetic":false,"types":[]},{"text":"impl Copy for msginfo","synthetic":false,"types":[]},{"text":"impl Copy for sembuf","synthetic":false,"types":[]},{"text":"impl Copy for input_event","synthetic":false,"types":[]},{"text":"impl Copy for input_id","synthetic":false,"types":[]},{"text":"impl Copy for input_absinfo","synthetic":false,"types":[]},{"text":"impl Copy for input_keymap_entry","synthetic":false,"types":[]},{"text":"impl Copy for input_mask","synthetic":false,"types":[]},{"text":"impl Copy for ff_replay","synthetic":false,"types":[]},{"text":"impl Copy for ff_trigger","synthetic":false,"types":[]},{"text":"impl Copy for ff_envelope","synthetic":false,"types":[]},{"text":"impl Copy for ff_constant_effect","synthetic":false,"types":[]},{"text":"impl Copy for ff_ramp_effect","synthetic":false,"types":[]},{"text":"impl Copy for ff_condition_effect","synthetic":false,"types":[]},{"text":"impl Copy for ff_periodic_effect","synthetic":false,"types":[]},{"text":"impl Copy for ff_rumble_effect","synthetic":false,"types":[]},{"text":"impl Copy for ff_effect","synthetic":false,"types":[]},{"text":"impl Copy for dl_phdr_info","synthetic":false,"types":[]},{"text":"impl Copy for Elf32_Ehdr","synthetic":false,"types":[]},{"text":"impl Copy for Elf64_Ehdr","synthetic":false,"types":[]},{"text":"impl Copy for Elf32_Sym","synthetic":false,"types":[]},{"text":"impl Copy for Elf64_Sym","synthetic":false,"types":[]},{"text":"impl Copy for Elf32_Phdr","synthetic":false,"types":[]},{"text":"impl Copy for Elf64_Phdr","synthetic":false,"types":[]},{"text":"impl Copy for Elf32_Shdr","synthetic":false,"types":[]},{"text":"impl Copy for Elf64_Shdr","synthetic":false,"types":[]},{"text":"impl Copy for Elf32_Chdr","synthetic":false,"types":[]},{"text":"impl Copy for Elf64_Chdr","synthetic":false,"types":[]},{"text":"impl Copy for ucred","synthetic":false,"types":[]},{"text":"impl Copy for mntent","synthetic":false,"types":[]},{"text":"impl Copy for posix_spawn_file_actions_t","synthetic":false,"types":[]},{"text":"impl Copy for posix_spawnattr_t","synthetic":false,"types":[]},{"text":"impl Copy for genlmsghdr","synthetic":false,"types":[]},{"text":"impl Copy for in6_pktinfo","synthetic":false,"types":[]},{"text":"impl Copy for arpd_request","synthetic":false,"types":[]},{"text":"impl Copy for inotify_event","synthetic":false,"types":[]},{"text":"impl Copy for fanotify_response","synthetic":false,"types":[]},{"text":"impl Copy for sockaddr_vm","synthetic":false,"types":[]},{"text":"impl Copy for regmatch_t","synthetic":false,"types":[]},{"text":"impl Copy for sock_extended_err","synthetic":false,"types":[]},{"text":"impl Copy for sockaddr_nl","synthetic":false,"types":[]},{"text":"impl Copy for dirent","synthetic":false,"types":[]},{"text":"impl Copy for dirent64","synthetic":false,"types":[]},{"text":"impl Copy for sockaddr_alg","synthetic":false,"types":[]},{"text":"impl Copy for af_alg_iv","synthetic":false,"types":[]},{"text":"impl Copy for mq_attr","synthetic":false,"types":[]},{"text":"impl Copy for statx","synthetic":false,"types":[]},{"text":"impl Copy for statx_timestamp","synthetic":false,"types":[]},{"text":"impl Copy for aiocb","synthetic":false,"types":[]},{"text":"impl Copy for __exit_status","synthetic":false,"types":[]},{"text":"impl Copy for __timeval","synthetic":false,"types":[]},{"text":"impl Copy for glob64_t","synthetic":false,"types":[]},{"text":"impl Copy for msghdr","synthetic":false,"types":[]},{"text":"impl Copy for cmsghdr","synthetic":false,"types":[]},{"text":"impl Copy for termios","synthetic":false,"types":[]},{"text":"impl Copy for mallinfo","synthetic":false,"types":[]},{"text":"impl Copy for nlmsghdr","synthetic":false,"types":[]},{"text":"impl Copy for nlmsgerr","synthetic":false,"types":[]},{"text":"impl Copy for nl_pktinfo","synthetic":false,"types":[]},{"text":"impl Copy for nl_mmap_req","synthetic":false,"types":[]},{"text":"impl Copy for nl_mmap_hdr","synthetic":false,"types":[]},{"text":"impl Copy for nlattr","synthetic":false,"types":[]},{"text":"impl Copy for rtentry","synthetic":false,"types":[]},{"text":"impl Copy for timex","synthetic":false,"types":[]},{"text":"impl Copy for ntptimeval","synthetic":false,"types":[]},{"text":"impl Copy for regex_t","synthetic":false,"types":[]},{"text":"impl Copy for utmpx","synthetic":false,"types":[]},{"text":"impl Copy for sigset_t","synthetic":false,"types":[]},{"text":"impl Copy for sysinfo","synthetic":false,"types":[]},{"text":"impl Copy for msqid_ds","synthetic":false,"types":[]},{"text":"impl Copy for sigaction","synthetic":false,"types":[]},{"text":"impl Copy for statfs","synthetic":false,"types":[]},{"text":"impl Copy for flock","synthetic":false,"types":[]},{"text":"impl Copy for flock64","synthetic":false,"types":[]},{"text":"impl Copy for siginfo_t","synthetic":false,"types":[]},{"text":"impl Copy for stack_t","synthetic":false,"types":[]},{"text":"impl Copy for stat","synthetic":false,"types":[]},{"text":"impl Copy for stat64","synthetic":false,"types":[]},{"text":"impl Copy for statfs64","synthetic":false,"types":[]},{"text":"impl Copy for statvfs64","synthetic":false,"types":[]},{"text":"impl Copy for pthread_attr_t","synthetic":false,"types":[]},{"text":"impl Copy for _libc_fpxreg","synthetic":false,"types":[]},{"text":"impl Copy for _libc_xmmreg","synthetic":false,"types":[]},{"text":"impl Copy for _libc_fpstate","synthetic":false,"types":[]},{"text":"impl Copy for user_regs_struct","synthetic":false,"types":[]},{"text":"impl Copy for user","synthetic":false,"types":[]},{"text":"impl Copy for mcontext_t","synthetic":false,"types":[]},{"text":"impl Copy for ipc_perm","synthetic":false,"types":[]},{"text":"impl Copy for shmid_ds","synthetic":false,"types":[]},{"text":"impl Copy for termios2","synthetic":false,"types":[]},{"text":"impl Copy for ip_mreqn","synthetic":false,"types":[]},{"text":"impl Copy for user_fpregs_struct","synthetic":false,"types":[]},{"text":"impl Copy for ucontext_t","synthetic":false,"types":[]},{"text":"impl Copy for statvfs","synthetic":false,"types":[]},{"text":"impl Copy for max_align_t","synthetic":false,"types":[]},{"text":"impl Copy for sem_t","synthetic":false,"types":[]},{"text":"impl Copy for pthread_mutexattr_t","synthetic":false,"types":[]},{"text":"impl Copy for pthread_rwlockattr_t","synthetic":false,"types":[]},{"text":"impl Copy for pthread_condattr_t","synthetic":false,"types":[]},{"text":"impl Copy for fanotify_event_metadata","synthetic":false,"types":[]},{"text":"impl Copy for pthread_cond_t","synthetic":false,"types":[]},{"text":"impl Copy for pthread_mutex_t","synthetic":false,"types":[]},{"text":"impl Copy for pthread_rwlock_t","synthetic":false,"types":[]},{"text":"impl Copy for in6_addr","synthetic":false,"types":[]}];
implementors["log"] = [{"text":"impl Copy for Level","synthetic":false,"types":[]},{"text":"impl Copy for LevelFilter","synthetic":false,"types":[]}];
implementors["mio"] = [{"text":"impl Copy for PollOpt","synthetic":false,"types":[]},{"text":"impl Copy for Ready","synthetic":false,"types":[]},{"text":"impl Copy for Event","synthetic":false,"types":[]},{"text":"impl Copy for UnixReady","synthetic":false,"types":[]},{"text":"impl Copy for Token","synthetic":false,"types":[]}];
implementors["notify"] = [{"text":"impl Copy for Op","synthetic":false,"types":[]},{"text":"impl Copy for RecursiveMode","synthetic":false,"types":[]}];
implementors["petgraph"] = [{"text":"impl Copy for Time","synthetic":false,"types":[]},{"text":"impl&lt;N:&nbsp;Copy&gt; Copy for DfsEvent&lt;N&gt;","synthetic":false,"types":[]},{"text":"impl&lt;B:&nbsp;Copy&gt; Copy for Control&lt;B&gt;","synthetic":false,"types":[]},{"text":"impl&lt;G:&nbsp;Copy, F:&nbsp;Copy&gt; Copy for NodeFiltered&lt;G, F&gt;","synthetic":false,"types":[]},{"text":"impl&lt;G:&nbsp;Copy, F:&nbsp;Copy&gt; Copy for EdgeFiltered&lt;G, F&gt;","synthetic":false,"types":[]},{"text":"impl&lt;G:&nbsp;Copy&gt; Copy for Reversed&lt;G&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;Copy&gt; Copy for ReversedEdgeReference&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;'a, E, Ty, Ix:&nbsp;Copy&gt; Copy for EdgeReference&lt;'a, E, Ty, Ix&gt;","synthetic":false,"types":[]},{"text":"impl&lt;Ix:&nbsp;Copy&gt; Copy for NodeIndex&lt;Ix&gt;","synthetic":false,"types":[]},{"text":"impl&lt;Ix:&nbsp;Copy&gt; Copy for EdgeIndex&lt;Ix&gt;","synthetic":false,"types":[]},{"text":"impl&lt;'a, E, Ix:&nbsp;IndexType&gt; Copy for EdgeReference&lt;'a, E, Ix&gt;","synthetic":false,"types":[]},{"text":"impl&lt;'a, E, Ix:&nbsp;IndexType&gt; Copy for EdgeReference&lt;'a, E, Ix&gt;","synthetic":false,"types":[]},{"text":"impl&lt;'b, T&gt; Copy for Ptr&lt;'b, T&gt;","synthetic":false,"types":[]},{"text":"impl Copy for Direction","synthetic":false,"types":[]},{"text":"impl Copy for Directed","synthetic":false,"types":[]},{"text":"impl Copy for Undirected","synthetic":false,"types":[]}];
implementors["regex"] = [{"text":"impl&lt;'t&gt; Copy for Match&lt;'t&gt;","synthetic":false,"types":[]},{"text":"impl&lt;'t&gt; Copy for Match&lt;'t&gt;","synthetic":false,"types":[]}];
implementors["regex_syntax"] = [{"text":"impl Copy for Span","synthetic":false,"types":[]},{"text":"impl Copy for Position","synthetic":false,"types":[]},{"text":"impl Copy for ClassSetBinaryOpKind","synthetic":false,"types":[]},{"text":"impl Copy for Flag","synthetic":false,"types":[]},{"text":"impl Copy for ClassUnicodeRange","synthetic":false,"types":[]},{"text":"impl Copy for ClassBytesRange","synthetic":false,"types":[]},{"text":"impl Copy for Utf8Sequence","synthetic":false,"types":[]},{"text":"impl Copy for Utf8Range","synthetic":false,"types":[]}];
implementors["ryu"] = [{"text":"impl Copy for Buffer","synthetic":false,"types":[]}];
implementors["serde"] = [{"text":"impl&lt;E&gt; Copy for UnitDeserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for BoolDeserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for I8Deserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for I16Deserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for I32Deserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for I64Deserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for IsizeDeserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for U8Deserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for U16Deserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for U64Deserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for UsizeDeserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for F32Deserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for F64Deserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for CharDeserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for I128Deserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for U128Deserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;E&gt; Copy for U32Deserializer&lt;E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;'de, E&gt; Copy for StrDeserializer&lt;'de, E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;'de, E&gt; Copy for BorrowedStrDeserializer&lt;'de, E&gt;","synthetic":false,"types":[]},{"text":"impl&lt;'de, E&gt; Copy for BorrowedBytesDeserializer&lt;'de, E&gt;","synthetic":false,"types":[]},{"text":"impl Copy for IgnoredAny","synthetic":false,"types":[]},{"text":"impl&lt;'a&gt; Copy for Unexpected&lt;'a&gt;","synthetic":false,"types":[]}];
implementors["serde_json"] = [{"text":"impl Copy for Category","synthetic":false,"types":[]}];
implementors["yaml_rust"] = [{"text":"impl Copy for EmitError","synthetic":false,"types":[]},{"text":"impl Copy for TEncoding","synthetic":false,"types":[]},{"text":"impl Copy for TScalarStyle","synthetic":false,"types":[]},{"text":"impl Copy for Marker","synthetic":false,"types":[]}];
if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()