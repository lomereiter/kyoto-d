module kyotocabinet;

import std.string;
import std.conv;
import std.exception;

extern(C) {

    struct KCDB {
        void *db;
    }

    struct KCCUR {
        void *cur;
    }

    struct KCSTR {
        char* buf;
        size_t size;
    }

    struct KCREC {
        KCSTR key;
        KCSTR value;
    }

    enum {
      KCOREADER = 1 << 0,                    /**< open as a reader */
      KCOWRITER = 1 << 1,                    /**< open as a writer */
      KCOCREATE = 1 << 2,                    /**< writer creating */
      KCOTRUNCATE = 1 << 3,                  /**< writer truncating */
      KCOAUTOTRAN = 1 << 4,                  /**< auto transaction */
      KCOAUTOSYNC = 1 << 5,                  /**< auto synchronization */
      KCONOLOCK = 1 << 6,                    /**< open without locking */
      KCOTRYLOCK = 1 << 7,                   /**< lock without blocking */
      KCONOREPAIR = 1 << 8                   /**< open without auto repair */
    }

    KCDB* kcdbnew();
    void kcdbdel(KCDB* db);
    int kcdbopen(KCDB* db, immutable(char)* path, uint mode);
    int kcdbclose(KCDB* db);
    int kcdbset(KCDB* db, immutable(char)* kbuf, size_t ksiz, immutable(char)* vbuf, size_t vsiz);
    char* kcdbget(KCDB* db, immutable(char)* kbuf, size_t ksiz, size_t* sp);
    int kcdbremove(KCDB* db, immutable(char)* kbuf, size_t ksiz);

    long kcdbsize(KCDB* db);
    long kcdbcount(KCDB* db);

    int kcdbecode(KCDB* db);
    char* kcdbemsg(KCDB* db);

    void kcfree(void* ptr);
}

unittest {

    import std.stdio;

    auto db = kcdbnew();
    auto filename = "/tmp/tmp.kch";
    if (!kcdbopen(db, toStringz(filename), KCOCREATE | KCOWRITER))
        printf("%s\n", kcdbemsg(db));

    auto key = "key";
    auto val = "value";
    if (!kcdbset(db, key.ptr, key.length, val.ptr, val.length))
        printf("%s\n", kcdbemsg(db));

    size_t len;
    auto ptr = kcdbget(db, key.ptr, key.length, &len);

    if (ptr is null)
        printf("%s\n", kcdbemsg(db));

    string str = ptr[0 .. len].idup;
    assert(str == "value");

    kcdbclose(db);
    kcdbdel(db);
}

class KyotoException : Exception {

    this(char* msg) {
        super(to!string(msg));
    }

    this(string msg) {
        super(msg);
    }
}

class Database {
    private {
        KCDB* db;
    }

    this(string filename, int mode) {
        db = kcdbnew();
        if (!kcdbopen(db, toStringz(filename), mode))
            throw new KyotoException(kcdbemsg(db));
    }

    ~this() {
        kcdbdel(db);
    }

    string opIndex(string key) {
        size_t len;
        auto ptr = kcdbget(db, key.ptr, key.length, &len);
        auto result = ptr[0 .. len].idup;
        kcfree(ptr);
        return result;
    }

    void opIndexAssign(string value, string key) {
        if (!kcdbset(db, key.ptr, key.length, value.ptr, value.length))
            throw new KyotoException(kcdbemsg(db));
    }

    bool remove(string key) {
        return cast(bool)kcdbremove(db, key.ptr, key.length);
    }

    long sizeInBytes() @property {
        return kcdbsize(db);
    }

    long count() @property {
        return kcdbcount(db);
    }

    bool close() {
        return cast(bool)kcdbclose(db);
    }
}

unittest {
    auto db = new Database("/tmp/tmp.kch", KCOCREATE | KCOWRITER);
    db["key"] = "value";
    assert(db.count == 1);
    assert(db["key"] == "value");
    db.remove("key");
    assert(db.count == 0);
    assert(db["key"] is null);
}
