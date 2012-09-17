package org.genouest.hadoopizer.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableFactories;

public class ObjectWritableComparable implements WritableComparable<ObjectWritableComparable> {

    private String keyType;
    private Class<?> declaredClass;
    private Object instance;

    public ObjectWritableComparable() {
        this.keyType = "";
    };
    
    public ObjectWritableComparable(String keyType, Object instance) {
        
        set(keyType, instance);
    }

    public ObjectWritableComparable(String keyType, Class<?> declaredClass, Object instance) {
        
        set(keyType, declaredClass, instance);
    }
    
    /** Reset the instance. */
    public void set(String keyType, Object instance) {

        this.instance = instance;
        this.declaredClass = instance.getClass();
        this.keyType = keyType;
    }
    
    /** Reset the instance. */
    public void set(String keyType, Class<?> declaredClass, Object instance) {

        this.instance = instance;
        this.keyType = keyType;
    }

    /** Return the instance, or null if none. */
    public Object get() { return instance; }
    
    /** Return the class this is meant to be. */
    public Class<?> getDeclaredClass() { return declaredClass; }
    
    public String toString() {
      return "OWC[type=" + keyType + "class=" + getDeclaredClass() + ",value=" + get() + "]";
    }

    public void readFields(DataInput in) throws IOException {
        readObject(in, this);
    }

    public void write(DataOutput out) throws IOException {
        writeObject(out, getKeyType(), get(), getDeclaredClass());
    }


    private static final Map<String, Class<?>> PRIMITIVE_NAMES = new HashMap<String, Class<?>>();
    static {
        PRIMITIVE_NAMES.put("boolean", Boolean.TYPE);
        PRIMITIVE_NAMES.put("byte", Byte.TYPE);
        PRIMITIVE_NAMES.put("char", Character.TYPE);
        PRIMITIVE_NAMES.put("short", Short.TYPE);
        PRIMITIVE_NAMES.put("int", Integer.TYPE);
        PRIMITIVE_NAMES.put("long", Long.TYPE);
        PRIMITIVE_NAMES.put("float", Float.TYPE);
        PRIMITIVE_NAMES.put("double", Double.TYPE);
        PRIMITIVE_NAMES.put("void", Void.TYPE);
    }

    private static class NullInstance implements Writable {
        private Class<?> declaredClass;
        @SuppressWarnings("unused")
        public NullInstance() { }
        public NullInstance(Class<?> declaredClass) {
            this.declaredClass = declaredClass;
        }
        public void readFields(DataInput in) throws IOException {
            String className = Text.readString(in);
            declaredClass = PRIMITIVE_NAMES.get(className);
            if (declaredClass == null) {
                try {
                    declaredClass = ObjectWritableComparable.getClassByName(className);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e.toString());
                }
            }
        }
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, declaredClass.getName());
        }
    }
    


    @SuppressWarnings("rawtypes")
    public static void writeObject(DataOutput out, String keyType, Object instance, Class<?> declaredClass) throws IOException {

        if (instance == null) {                       // null
            instance = new NullInstance(declaredClass);
            declaredClass = Writable.class;
        }
        
        Text.writeString(out, keyType); // always write declared

        Text.writeString(out, declaredClass.getName()); // always write declared

        if (declaredClass.isArray()) {                // array
            int length = Array.getLength(instance);
            out.writeInt(length);
            for (int i = 0; i < length; i++) {
                writeObject(out, keyType, Array.get(instance, i),
                        declaredClass.getComponentType());
            }

        } else if (declaredClass == String.class) {   // String
            Text.writeString(out, (String)instance);

        } else if (declaredClass.isPrimitive()) {     // primitive type

            if (declaredClass == Boolean.TYPE) {        // boolean
                out.writeBoolean(((Boolean)instance).booleanValue());
            } else if (declaredClass == Character.TYPE) { // char
                out.writeChar(((Character)instance).charValue());
            } else if (declaredClass == Byte.TYPE) {    // byte
                out.writeByte(((Byte)instance).byteValue());
            } else if (declaredClass == Short.TYPE) {   // short
                out.writeShort(((Short)instance).shortValue());
            } else if (declaredClass == Integer.TYPE) { // int
                out.writeInt(((Integer)instance).intValue());
            } else if (declaredClass == Long.TYPE) {    // long
                out.writeLong(((Long)instance).longValue());
            } else if (declaredClass == Float.TYPE) {   // float
                out.writeFloat(((Float)instance).floatValue());
            } else if (declaredClass == Double.TYPE) {  // double
                out.writeDouble(((Double)instance).doubleValue());
            } else if (declaredClass == Void.TYPE) {    // void
            } else {
                throw new IllegalArgumentException("Not a primitive: "+declaredClass);
            }
        } else if (declaredClass.isEnum()) {         // enum
            Text.writeString(out, ((Enum)instance).name());
        } else if (Writable.class.isAssignableFrom(declaredClass)) { // Writable
            Text.writeString(out, instance.getClass().getName());
            ((Writable)instance).write(out);

        } else {
            throw new IOException("Can't write: "+instance+" as "+declaredClass);
        }
    }


    /** Read a {@link Writable}, {@link String}, primitive type, or an array of
     * the preceding. */
    public static Object readObject(DataInput in) throws IOException {
        
        return readObject(in, null);
    }

    /** Read a {@link Writable}, {@link String}, primitive type, or an array of
     * the preceding. */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Object readObject(DataInput in, ObjectWritableComparable objectWritable) throws IOException {
        
        String keyType = Text.readString(in);
        String className = Text.readString(in);
        Class<?> declaredClass = PRIMITIVE_NAMES.get(className);
        if (declaredClass == null) {
            try {
                declaredClass = getClassByName(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("readObject can't find class " + className, e);
            }
        }    

        Object instance;

        if (declaredClass.isPrimitive()) {            // primitive types

            if (declaredClass == Boolean.TYPE) {             // boolean
                instance = Boolean.valueOf(in.readBoolean());
            } else if (declaredClass == Character.TYPE) {    // char
                instance = Character.valueOf(in.readChar());
            } else if (declaredClass == Byte.TYPE) {         // byte
                instance = Byte.valueOf(in.readByte());
            } else if (declaredClass == Short.TYPE) {        // short
                instance = Short.valueOf(in.readShort());
            } else if (declaredClass == Integer.TYPE) {      // int
                instance = Integer.valueOf(in.readInt());
            } else if (declaredClass == Long.TYPE) {         // long
                instance = Long.valueOf(in.readLong());
            } else if (declaredClass == Float.TYPE) {        // float
                instance = Float.valueOf(in.readFloat());
            } else if (declaredClass == Double.TYPE) {       // double
                instance = Double.valueOf(in.readDouble());
            } else if (declaredClass == Void.TYPE) {         // void
                instance = null;
            } else {
                throw new IllegalArgumentException("Not a primitive: "+declaredClass);
            }

        } else if (declaredClass.isArray()) {              // array
            int length = in.readInt();
            instance = Array.newInstance(declaredClass.getComponentType(), length);
            for (int i = 0; i < length; i++) {
                Array.set(instance, i, readObject(in));
            }

        } else if (declaredClass == String.class) {        // String
            instance = Text.readString(in);
        } else if (declaredClass.isEnum()) {         // enum
            instance = Enum.valueOf((Class<? extends Enum>) declaredClass, Text.readString(in));
        } else {                                      // Writable
            Class instanceClass = null;
            String str = "";
            try {
                str = Text.readString(in);
                instanceClass = getClassByName(str);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("readObject can't find class " + str, e);
            }

            Writable writable = WritableFactories.newInstance(instanceClass);
            writable.readFields(in);
            instance = writable;

            if (instanceClass == NullInstance.class) {  // null
                declaredClass = ((NullInstance)instance).declaredClass;
                instance = null;
            }
        }

        if (objectWritable != null) {                 // store values
            objectWritable.set(keyType, instance);
        }

        return instance;
    }

    @Override
    public int compareTo(ObjectWritableComparable o) {

        if (this.equals(o))
            return 0;
        
        int typeComp = getKeyType().compareTo(o.getKeyType());
        if (typeComp != 0)
            return typeComp;
        
        if (!getDeclaredClass().equals(o.getDeclaredClass()))
            return getDeclaredClass().getName().compareTo(o.getDeclaredClass().getName());
        
        return get().toString().compareTo(o.get().toString());
    }
    
    @Override
    public int hashCode() {
        
        return get().hashCode() + getDeclaredClass().hashCode() + getKeyType().hashCode();
    }

    @Override
    public boolean equals(Object obj) {

        if (!(obj instanceof ObjectWritableComparable))
            return false;

        ObjectWritableComparable that = (ObjectWritableComparable) obj;
        
        return (that.get().equals(obj) && (that.getKeyType().compareTo(this.getKeyType()) == 0) && that.getDeclaredClass().equals(that.getDeclaredClass()));
    }

    /**
     * @return the keyType
     */
    public String getKeyType() {
        return keyType;
    }

    /**
     * @param keyType the keyType to set
     */
    public void setKeyType(String type) {
        this.keyType = type;
    }
    
    /**
     * Load a class by name.
     * 
     * @param name the class name.
     * @return the class object.
     * @throws ClassNotFoundException if the class is not found.
     */
    protected static Class<?> getClassByName(String name) throws ClassNotFoundException {
        
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = Configuration.class.getClassLoader();
        }
        
        return Class.forName(name, true, classLoader);
    }
}
