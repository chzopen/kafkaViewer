package per.chzopen.kafkaViewer.gui;

import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;

import javax.swing.JTextField;

import org.apache.commons.lang3.math.NumberUtils;

public class JNumberField extends JTextField
{
	
	private static final long serialVersionUID = 1L;

	private JNumberField _this = this;
	
	private Type type = Type.UINT;
	
	private String defaultValue = "0";
	
	public JNumberField(Type type, String value)
	{
		this.type = type;
		
		validateValue(value);
		this.setText(value);
		
		this.addFocusListener(new FocusAdapter()
		{
			public void focusLost(FocusEvent e)
			{
				_this.setText(_this.getText().trim());
				if( !checkValue(_this.getText()) )
				{
					_this.setText(defaultValue);
				}
			}
		});
		
	}
	
	public boolean checkValue(String value)
	{
		if( type==Type.INT && !isInt(value) )
		{
			return false;
		}
		else if( type==Type.UINT && !isUInt(value) )
		{
			return false;
		} 
		else if( type==Type.FLOAT && !isFloat(value) )
		{
			return false;
		} 
		else if( type==Type.UFLOAT && !isUFloat(value) )
		{
			return false;
		} 
		else
		{
			return true;
		}
	}
	
	public void validateValue(String value)
	{
		if( !checkValue(value) )
		{
			throw new IllegalArgumentException("value: " + value);
		}
	}
	
	public void setDefaultValue(String value)
	{
		validateValue(value);
		this.defaultValue = value;
	}
	
	public long getLong()
	{
		return Long.parseLong(this.getText());
	}

	public double getDouble()
	{
		return Double.parseDouble(this.getText());
	}
	
	private static boolean isInt(String str)
	{
		if( str.startsWith("-") )
		{
			str = str.substring(1);
		}
		return NumberUtils.isDigits(str);
	}

	private static boolean isUInt(String str)
	{
		return NumberUtils.isDigits(str);
	}

	private static boolean isUFloat(String str)
	{
		return NumberUtils.isNumber(str);
	}

	private static boolean isFloat(String str)
	{
		if( str.startsWith("-") )
		{
			str = str.substring(1);
		}
		return NumberUtils.isNumber(str);
	}
	
	public static enum Type
	{
		INT,
		UINT,
		FLOAT,
		UFLOAT
	}
	
}
