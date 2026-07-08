use std::error::Error;
use std::os::raw::{c_char, c_int, c_long, c_ulong, c_void};

const XDF_IGNORE_WHITESPACE: c_ulong = 1 << 1;
const XDF_IGNORE_WHITESPACE_CHANGE: c_ulong = 1 << 2;
const XDF_IGNORE_WHITESPACE_AT_EOL: c_ulong = 1 << 3;
const XDF_IGNORE_CR_AT_EOL: c_ulong = 1 << 4;
const XDF_WHITESPACE_FLAGS: c_ulong = XDF_IGNORE_WHITESPACE
    | XDF_IGNORE_WHITESPACE_CHANGE
    | XDF_IGNORE_WHITESPACE_AT_EOL
    | XDF_IGNORE_CR_AT_EOL;
const XDL_EMIT_NO_HUNK_HDR: c_ulong = 1 << 1;

#[repr(C)]
struct mmfile_t {
    ptr: *mut c_char,
    size: c_long,
}

#[repr(C)]
struct mmbuffer_t {
    ptr: *mut c_char,
    size: c_long,
}

#[repr(C)]
struct xpparam_t {
    flags: c_ulong,
    ignore_regex: *mut c_void,
    ignore_regex_nr: usize,
    anchors: *mut c_void,
    anchors_nr: usize,
}

#[repr(C)]
struct xdemitconf_t {
    ctxlen: c_long,
    interhunkctxlen: c_long,
    flags: c_ulong,
    find_func: Option<
        unsafe extern "C" fn(*const c_char, c_long, *mut c_char, c_long, *mut c_void) -> c_long,
    >,
    find_func_priv: *mut c_void,
    hunk_func: Option<unsafe extern "C" fn(c_long, c_long, c_long, c_long, *mut c_void) -> c_int>,
}

#[repr(C)]
struct xdemitcb_t {
    priv_: *mut c_void,
    out_hunk: Option<
        unsafe extern "C" fn(
            *mut c_void,
            c_long,
            c_long,
            c_long,
            c_long,
            *const c_char,
            c_long,
        ) -> c_int,
    >,
    out_line: Option<unsafe extern "C" fn(*mut c_void, *mut mmbuffer_t, c_int) -> c_int>,
}

extern "C" {
    fn xdl_diff(
        mf1: *mut mmfile_t,
        mf2: *mut mmfile_t,
        xpp: *const xpparam_t,
        xecfg: *const xdemitconf_t,
        ecb: *mut xdemitcb_t,
    ) -> c_int;
}

struct LineCounter {
    added: i32,
    deleted: i32,
}

unsafe extern "C" fn count_lines_cb(
    priv_: *mut c_void,
    bufs: *mut mmbuffer_t,
    _count: c_int,
) -> c_int {
    let counter = &mut *(priv_ as *mut LineCounter);
    let sign = *(*bufs).ptr;
    if sign == b'+' as c_char {
        counter.added += 1;
    } else if sign == b'-' as c_char {
        counter.deleted += 1;
    }
    0
}

fn is_binary(content: &[u8]) -> bool {
    let len = content.len().min(8000);
    content[..len].contains(&0)
}

/// Trim identical bytes from the end of both slices, breaking at a newline.
/// Ported from git's `trim_common_tail` (xdiff-interface.c).
fn trim_common_tail(a: &[u8], b: &[u8]) -> (usize, usize) {
    const BLK: usize = 1024;
    let smaller = a.len().min(b.len());
    let mut trimmed: usize = 0;

    while trimmed + BLK <= smaller
        && a[a.len() - trimmed - BLK..a.len() - trimmed]
            == b[b.len() - trimmed - BLK..b.len() - trimmed]
    {
        trimmed += BLK;
    }

    let mut recovered: usize = 0;
    while recovered < trimmed {
        recovered += 1;
        if a[a.len() - trimmed + recovered - 1] == b'\n' {
            break;
        }
    }

    (a.len() - trimmed + recovered, b.len() - trimmed + recovered)
}

/// Requires libgit2 to be initialized (xdiff uses git__malloc internally).
pub fn diff_line_counts(
    old: &[u8],
    new: &[u8],
    ignore_whitespace: bool,
) -> Result<(i32, i32), Box<dyn Error>> {
    if is_binary(old) || is_binary(new) {
        return Ok((0, 0));
    }

    let (old_len, new_len) = trim_common_tail(old, new);

    let mut mf1 = mmfile_t {
        ptr: old.as_ptr() as *mut c_char,
        size: old_len as c_long,
    };
    let mut mf2 = mmfile_t {
        ptr: new.as_ptr() as *mut c_char,
        size: new_len as c_long,
    };

    let xpp = xpparam_t {
        flags: if ignore_whitespace {
            XDF_WHITESPACE_FLAGS
        } else {
            0
        },
        ignore_regex: std::ptr::null_mut(),
        ignore_regex_nr: 0,
        anchors: std::ptr::null_mut(),
        anchors_nr: 0,
    };

    let xecfg = xdemitconf_t {
        ctxlen: 0,
        interhunkctxlen: 0,
        flags: XDL_EMIT_NO_HUNK_HDR,
        find_func: None,
        find_func_priv: std::ptr::null_mut(),
        hunk_func: None,
    };

    let mut counter = LineCounter {
        added: 0,
        deleted: 0,
    };

    let mut ecb = xdemitcb_t {
        priv_: &mut counter as *mut _ as *mut c_void,
        out_hunk: None,
        out_line: Some(count_lines_cb),
    };

    let ret = unsafe { xdl_diff(&mut mf1, &mut mf2, &xpp, &xecfg, &mut ecb) };

    if ret != 0 {
        return Err("xdl_diff failed".into());
    }

    Ok((counter.added, counter.deleted))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Vendored libgit2 version expected by the xdiff FFI bindings (git2 0.20.2).
    const EXPECTED_LIBGIT2_VERSION: (i32, i32, i32) = (1, 9, 1);

    extern "C" {
        fn git_libgit2_version(major: *mut c_int, minor: *mut c_int, rev: *mut c_int) -> c_int;
    }

    #[test]
    fn libgit2_version_matches_expected() {
        let mut major = 0;
        let mut minor = 0;
        let mut rev = 0;
        let ret = unsafe {
            git_libgit2_version(
                &mut major as *mut c_int,
                &mut minor as *mut c_int,
                &mut rev as *mut c_int,
            )
        };
        assert_eq!(ret, 0);
        assert_eq!(
            (major, minor, rev),
            EXPECTED_LIBGIT2_VERSION,
            "update EXPECTED_LIBGIT2_VERSION and xdiff FFI bindings when bumping git2"
        );
    }

    #[test]
    fn added_file() {
        let _ = git2::Repository::open(".");
        let (add, del) = diff_line_counts(b"", b"line1\nline2\nline3\n", false).unwrap();
        assert_eq!((add, del), (3, 0));
    }

    #[test]
    fn deleted_file() {
        let _ = git2::Repository::open(".");
        let (add, del) = diff_line_counts(b"line1\nline2\n", b"", false).unwrap();
        assert_eq!((add, del), (0, 2));
    }

    #[test]
    fn modified_file() {
        let _ = git2::Repository::open(".");
        let (add, del) = diff_line_counts(b"aaa\nbbb\nccc\n", b"aaa\nBBB\nccc\n", false).unwrap();
        assert_eq!((add, del), (1, 1));
    }

    #[test]
    fn binary_returns_zero() {
        let _ = git2::Repository::open(".");
        let (add, del) = diff_line_counts(b"\x00binary", b"text\n", false).unwrap();
        assert_eq!((add, del), (0, 0));
    }

    #[test]
    fn ignore_whitespace() {
        let _ = git2::Repository::open(".");
        let (add, del) = diff_line_counts(b"hello world\n", b"hello  world\n", true).unwrap();
        assert_eq!((add, del), (0, 0));
        let (add, del) = diff_line_counts(b"hello world\n", b"hello  world\n", false).unwrap();
        assert_eq!((add, del), (1, 1));
    }

    #[test]
    fn ignore_whitespace_crlf() {
        let _ = git2::Repository::open(".");
        let (add, del) = diff_line_counts(b"line\r\n", b"line\n", true).unwrap();
        assert_eq!((add, del), (0, 0));
    }

    #[test]
    fn ignore_whitespace_trailing() {
        let _ = git2::Repository::open(".");
        let (add, del) = diff_line_counts(b"line  \n", b"line\n", true).unwrap();
        assert_eq!((add, del), (0, 0));
    }

    #[test]
    fn identical_files() {
        let _ = git2::Repository::open(".");
        let (add, del) = diff_line_counts(b"same\n", b"same\n", false).unwrap();
        assert_eq!((add, del), (0, 0));
    }

    #[test]
    fn both_empty() {
        let _ = git2::Repository::open(".");
        let (add, del) = diff_line_counts(b"", b"", false).unwrap();
        assert_eq!((add, del), (0, 0));
    }
}
