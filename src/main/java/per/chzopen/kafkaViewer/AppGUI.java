package per.chzopen.kafkaViewer;

import java.awt.Font;

import javax.swing.JFrame;
import javax.swing.UIManager;

import per.chzopen.kafkaViewer.gui.GuiResources;
import per.chzopen.kafkaViewer.gui.KafkaViewerFrame;

/**
 * 
 */
public class AppGUI 
{
	private static void guiInit()
	{
		Font font = new Font("宋体", Font.PLAIN, 18);
        UIManager.put("Button.font",      font); 
        UIManager.put("Label.font",       font); 
        UIManager.put("TextField.font",   font); 
        UIManager.put("TextArea.font",    font);
        UIManager.put("TableHeader.font", font);
        UIManager.put("MenuBar.font",     font);
        UIManager.put("Menu.font",        font);
        UIManager.put("MenuItem.font",    font);
        UIManager.put("RadioButton.font", font);
        UIManager.put("TabbedPane.font",  font);
	}
	
    public static void main( String[] args ) throws Exception
    {
    	guiInit();
    	
    	KafkaViewerFrame frame = new KafkaViewerFrame();
    	frame.setSize(555, 555);
    	frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    	frame.setExtendedState(JFrame.MAXIMIZED_BOTH);
    	frame.setVisible(true);
    	
    	GuiResources.kafkaViewerFrame = frame;
    }
}